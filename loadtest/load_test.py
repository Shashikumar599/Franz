"""Load test for the read path: GET /top-liked?minutes=M&limit=K on the Bluesky query service.

Every request is a random query, e.g. "top 6 in the last 13 minutes":
    minutes ~ uniform 1..60, limit (k) ~ uniform 1..K_MAX

The test is OPEN-LOOP: requests are sent on a fixed schedule whether or not earlier
ones have come back, like real independent users. Latency is measured from the
*scheduled* send time, so if the client or the server falls behind, the delay shows up
in the numbers instead of being hidden (this is the "coordinated omission" problem
that closed-loop tools such as plain `ab` have).

Plan (each test 3 min by default, 30 s cool-down between tests):
    T1 uniform 100/s   T2 uniform 200/s   T3 uniform 300/s
    T4 poisson 300/s   (random gaps, same average)
    T5 burst   300/s avg  (1 s at 900/s every 5 s, 150/s in between)
    T6 uniform 500/s   T7 burst 500/s avg (peaks at 1500/s)

Usage (from the repo root, host venv):
    python loadtest/load_test.py                    # full plan, ~25 min
    python loadtest/load_test.py --only T1 T2       # a subset
    python loadtest/load_test.py --duration 30      # short smoke run of every test
    python loadtest/load_test.py --stop-on-fail     # stop at the first test that fails

Output: live line every 10 s, a summary table at the end, and files in
loadtest/results/<timestamp>/ (summary.csv, summary.json, one requests_<test>.csv each).
"""
import argparse
import asyncio
import csv
import json
import random
import time
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import aiohttp

# ---------------------------------------------------------------------------- plan

@dataclass
class Test:
    name: str
    pattern: str        # uniform | poisson | burst
    rate: float         # average requests per second


PLAN = [
    Test("T1", "uniform", 100),
    Test("T2", "uniform", 200),
    Test("T3", "uniform", 300),
    Test("T4", "poisson", 300),
    Test("T5", "burst", 300),
    Test("T6", "uniform", 500),
    Test("T7", "burst", 500),
]

# Burst shape: every BURST_PERIOD_S seconds, BURST_LEN_S seconds run at BURST_FACTOR x the
# average rate; the rest of the period runs slower so the average still equals `rate`.
BURST_PERIOD_S = 5.0
BURST_LEN_S = 1.0
BURST_FACTOR = 3.0

# A test "fails" if any of these is crossed. They are only labels for the summary.
MAX_ERROR_RATE = 0.01       # 1 % non-200 or timeouts
MAX_P99_MS = 500.0
MIN_ACHIEVED = 0.95         # completed rate / target rate

MINUTES_MIN, MINUTES_MAX = 1, 60   # the API accepts 1..60
K_MAX_DEFAULT = 20                 # the API accepts 1..100


def schedule(pattern, rate, duration_s, rng):
    """Planned send times in seconds from the start of the test."""
    times = []
    if pattern == "uniform":
        step = 1.0 / rate
        n = int(duration_s * rate)
        times = [i * step for i in range(n)]
    elif pattern == "poisson":
        # Exponential gaps: the arrival pattern of many independent users.
        t = rng.expovariate(rate)
        while t < duration_s:
            times.append(t)
            t += rng.expovariate(rate)
    elif pattern == "burst":
        high = rate * BURST_FACTOR
        low = rate * (BURST_PERIOD_S - BURST_FACTOR * BURST_LEN_S) / (BURST_PERIOD_S - BURST_LEN_S)
        t = 0.0
        while t < duration_s:
            times.append(t)
            in_burst = (t % BURST_PERIOD_S) < BURST_LEN_S
            t += 1.0 / (high if in_burst else low)
    else:
        raise ValueError(pattern)
    return times


# ---------------------------------------------------------------------------- one request

@dataclass
class Result:
    planned: float          # scheduled send time (perf_counter clock)
    started: float          # when the request actually left
    ended: float
    minutes: int
    k: int
    status: str             # "200", "503", ..., "timeout", "conn_error"
    cache: str              # X-Cache header: HIT | MISS | WAIT | -
    items: int              # number of items returned

    @property
    def latency_ms(self):           # what a user sees: from the moment they "clicked"
        return (self.ended - self.planned) * 1000

    @property
    def service_ms(self):           # from the moment the request left the client
        return (self.ended - self.started) * 1000

    @property
    def lag_ms(self):               # how late the client was in sending (client health)
        return (self.started - self.planned) * 1000


async def fire(session, url, planned, minutes, k, timeout_s, out, inflight):
    inflight[0] += 1
    started = time.perf_counter()
    status, cache, items = "conn_error", "-", 0
    try:
        async with session.get(url, params={"minutes": minutes, "limit": k},
                               timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
            body = await resp.read()
            status = str(resp.status)
            cache = resp.headers.get("X-Cache", "-")
            if resp.status == 200:
                items = len(json.loads(body)["items"])
    except asyncio.TimeoutError:
        status = "timeout"
    except aiohttp.ClientError:
        status = "conn_error"
    finally:
        inflight[0] -= 1
    out.append(Result(planned, started, time.perf_counter(), minutes, k, status, cache, items))


# ---------------------------------------------------------------------------- stats

def pct(sorted_vals, p):
    if not sorted_vals:
        return float("nan")
    i = min(len(sorted_vals) - 1, max(0, int(round(p / 100 * (len(sorted_vals) - 1)))))
    return sorted_vals[i]


def summarize(results: List[Result], seconds: float):
    ok = [r for r in results if r.status == "200"]
    lat = sorted(r.latency_ms for r in ok)
    lag = sorted(r.lag_ms for r in results)
    statuses = Counter(r.status for r in results)
    caches = Counter(r.cache for r in ok)
    n = len(results)
    return {
        "requests": n,
        "ok": len(ok),
        "errors": n - len(ok),
        "error_rate": (n - len(ok)) / n if n else 0.0,
        "achieved_rps": len(ok) / seconds if seconds > 0 else 0.0,
        "p50_ms": pct(lat, 50), "p90_ms": pct(lat, 90), "p99_ms": pct(lat, 99),
        "p999_ms": pct(lat, 99.9), "max_ms": lat[-1] if lat else float("nan"),
        "client_lag_p99_ms": pct(lag, 99),
        "statuses": dict(statuses),
        "cache": {k: round(v / len(ok) * 100, 2) for k, v in caches.items()} if ok else {},
    }


def live_line(test, elapsed, window: List[Result], window_s, inflight, sent):
    s = summarize(window, window_s)
    cache = " ".join(f"{k} {v:.1f}%" for k, v in sorted(s["cache"].items()))
    errs = {k: v for k, v in s["statuses"].items() if k != "200"}
    return (f"[{test.name} {test.pattern} {test.rate:g}/s] t={elapsed:4.0f}s  sent {sent:6d}  "
            f"rps {s['achieved_rps']:6.1f}  err {s['errors']:4d}{' ' + str(errs) if errs else ''}  "
            f"p50 {s['p50_ms']:6.1f}  p90 {s['p90_ms']:6.1f}  p99 {s['p99_ms']:7.1f}  max {s['max_ms']:7.1f} ms  "
            f"| {cache} | inflight {inflight}  client-lag p99 {s['client_lag_p99_ms']:.1f} ms")


# ---------------------------------------------------------------------------- run one test

async def run_test(test, args, session, rng, outdir):
    times = schedule(test.pattern, test.rate, args.duration, rng)
    queries = [(rng.randint(MINUTES_MIN, MINUTES_MAX), rng.randint(1, args.k_max)) for _ in times]
    url = args.url.rstrip("/") + "/top-liked"

    results: List[Result] = []
    inflight = [0]
    tasks = set()
    t0 = time.perf_counter() + 0.2
    print(f"\n=== {test.name}: {test.pattern} {test.rate:g}/s for {args.duration:.0f} s "
          f"({len(times)} requests) ===")

    async def reporter():
        last_i, last_t = 0, t0
        while True:
            await asyncio.sleep(args.report_every)
            now = time.perf_counter()
            window = results[last_i:]
            last_i = len(results)
            print(live_line(test, now - t0, window, now - last_t, inflight[0], sent[0]), flush=True)
            last_t = now

    sent = [0]
    rep = asyncio.create_task(reporter())
    for offset, (minutes, k) in zip(times, queries):
        target = t0 + offset
        delay = target - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        task = asyncio.create_task(fire(session, url, target, minutes, k, args.timeout, results, inflight))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        sent[0] += 1
    send_end = time.perf_counter()
    if tasks:
        await asyncio.wait(list(tasks), timeout=args.timeout + 1)   # let the tail finish
    rep.cancel()

    summary = summarize(results, send_end - t0)
    wall_now, perf_now = time.time(), time.perf_counter()     # to line up with monitor.py's unix ts
    summary.update(test=test.name, pattern=test.pattern, target_rps=test.rate,
                   duration_s=args.duration,
                   started_at=wall_now - (perf_now - t0), ended_at=wall_now - (perf_now - send_end))
    failures = []
    if summary["error_rate"] > MAX_ERROR_RATE:
        failures.append(f"errors {summary['error_rate']:.1%}")
    if summary["p99_ms"] > MAX_P99_MS or summary["p99_ms"] != summary["p99_ms"]:
        failures.append(f"p99 {summary['p99_ms']:.0f} ms")
    if summary["achieved_rps"] < MIN_ACHIEVED * test.rate:
        failures.append(f"achieved {summary['achieved_rps']:.0f}/s")
    if summary["client_lag_p99_ms"] > 20:
        failures.append("CLIENT is lagging: numbers above may be the load generator, not the server")
    summary["verdict"] = "PASS" if not failures else "FAIL: " + "; ".join(failures)

    with open(outdir / f"requests_{test.name}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["planned_s", "minutes", "k", "status", "cache", "items",
                    "latency_ms", "service_ms", "client_lag_ms"])
        for r in sorted(results, key=lambda r: r.planned):
            w.writerow([f"{r.planned - t0:.4f}", r.minutes, r.k, r.status, r.cache, r.items,
                        f"{r.latency_ms:.2f}", f"{r.service_ms:.2f}", f"{r.lag_ms:.2f}"])
    print(f"--- {test.name} {summary['verdict']}  ok {summary['ok']}/{summary['requests']}  "
          f"rps {summary['achieved_rps']:.1f}  p50 {summary['p50_ms']:.1f}  p99 {summary['p99_ms']:.1f}  "
          f"p99.9 {summary['p999_ms']:.1f}  max {summary['max_ms']:.1f} ms  cache {summary['cache']}")
    return summary


# ---------------------------------------------------------------------------- main

async def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--duration", type=float, default=180, help="seconds per test (default 180)")
    ap.add_argument("--cooldown", type=float, default=30, help="pause between tests (default 30)")
    ap.add_argument("--only", nargs="*", help="run only these tests, e.g. --only T1 T3")
    ap.add_argument("--k-max", type=int, default=K_MAX_DEFAULT, help="k is random in 1..k-max (API max 100)")
    ap.add_argument("--timeout", type=float, default=5.0, help="per-request timeout, seconds")
    ap.add_argument("--report-every", type=float, default=10.0)
    ap.add_argument("--seed", type=int, default=42, help="same seed = same query sequence")
    ap.add_argument("--stop-on-fail", action="store_true")
    ap.add_argument("--out", help="results directory (default loadtest/results/<timestamp>)")
    args = ap.parse_args()

    plan = [t for t in PLAN if not args.only or t.name in args.only]
    rng = random.Random(args.seed)
    outdir = (Path(args.out) if args.out else
              Path(__file__).resolve().parent / "results" / datetime.now().strftime("%Y%m%d-%H%M%S"))
    outdir.mkdir(parents=True, exist_ok=True)
    print(f"target {args.url}   results -> {outdir}")
    print(f"queries: minutes uniform {MINUTES_MIN}..{MINUTES_MAX}, k uniform 1..{args.k_max}")

    # Many connections: an open-loop test must never queue behind a small pool.
    connector = aiohttp.TCPConnector(limit=4000, ttl_dns_cache=300)
    summaries = []
    async with aiohttp.ClientSession(connector=connector) as session:
        for i, test in enumerate(plan):
            if i:
                print(f"\ncool-down {args.cooldown:.0f} s")
                await asyncio.sleep(args.cooldown)
            s = await run_test(test, args, session, rng, outdir)
            summaries.append(s)
            if args.stop_on_fail and s["verdict"] != "PASS":
                print("stopping: --stop-on-fail")
                break

    cols = ["test", "pattern", "target_rps", "achieved_rps", "ok", "errors", "error_rate",
            "p50_ms", "p90_ms", "p99_ms", "p999_ms", "max_ms", "client_lag_p99_ms", "verdict"]
    with open(outdir / "summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(summaries)
    (outdir / "summary.json").write_text(json.dumps(summaries, indent=2))

    print("\n" + "=" * 110)
    print(f"{'test':5} {'pattern':8} {'target':>7} {'got':>7} {'err%':>6} {'p50':>7} {'p90':>7} "
          f"{'p99':>8} {'p99.9':>8} {'max':>8}  verdict")
    for s in summaries:
        print(f"{s['test']:5} {s['pattern']:8} {s['target_rps']:7.0f} {s['achieved_rps']:7.1f} "
              f"{s['error_rate'] * 100:6.2f} {s['p50_ms']:7.1f} {s['p90_ms']:7.1f} {s['p99_ms']:8.1f} "
              f"{s['p999_ms']:8.1f} {s['max_ms']:8.1f}  {s['verdict']}")
    print(f"latency in ms, measured from the scheduled send time.  files: {outdir}")


if __name__ == "__main__":
    asyncio.run(main())
