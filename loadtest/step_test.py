"""Step (ramp) stress test with vegeta: raise a steady request rate until the system breaks.

Each step runs vegeta open-loop at a fixed rate for STEP_SECONDS with random queries
(minutes 1..60, k 1..20, seeded). After each step it reports, side by side:
  - client latency  (vegeta: request sent -> response received, includes port forwarding)
  - server latency  (X-Server-Ms header set by the API: request received -> response headers)
  - achieved rate, errors, cache mix
and stops after the first step that breaks (error rate > 1 %, or < 95 % of the target rate
served). Comparing client and server latency shows where time is spent.

Usage (repo root, host venv; needs `vegeta` on PATH):
    python loadtest/step_test.py --out loadtest/results/<run>
    python loadtest/step_test.py --rates 500 1000 2000 --step-seconds 30 --out ...
Output in --out: steps.csv, steps.json, step_<rate>.json (vegeta's own report), console table.
"""
import argparse
import csv
import json
import random
import subprocess
import time
from collections import Counter
from pathlib import Path

DEFAULT_RATES = [500, 750, 1000, 1250, 1500, 2000, 2500, 3000, 4000, 5000]
MAX_ERROR_RATE = 0.01
MIN_ACHIEVED = 0.95


def pct(vals, p):
    if not vals:
        return float("nan")
    return vals[min(len(vals) - 1, int(round(p / 100 * (len(vals) - 1))))]


def make_targets(path, url, n, seed):
    rng = random.Random(seed)
    with open(path, "w") as f:
        for _ in range(n):
            f.write(f"GET {url}/top-liked?minutes={rng.randint(1, 60)}&limit={rng.randint(1, 20)}\n")


def run_step(rate, args, targets, out):
    """One vegeta attack at `rate`/s. Returns a summary dict."""
    started = time.time()
    attack = subprocess.Popen(
        ["vegeta", "attack", f"-rate={rate}/s", f"-duration={args.step_seconds}s",
         f"-targets={targets}", f"-timeout={args.timeout}s", "-max-body=0",
         "-keepalive=true", f"-name=r{rate}"],
        stdout=subprocess.PIPE)
    encode = subprocess.Popen(["vegeta", "encode", "--to", "json"], stdin=attack.stdout,
                              stdout=subprocess.PIPE, text=True)
    attack.stdout.close()

    client, server, codes, cache, errors = [], [], Counter(), Counter(), Counter()
    first_ts = last_ts = None
    for line in encode.stdout:
        r = json.loads(line)
        codes[r["code"]] += 1
        if r["error"]:
            errors[r["error"].split(":")[-1].strip()[:60]] += 1
        if r["code"] == 200:
            client.append(r["latency"] / 1e6)
            h = r.get("headers") or {}
            if "X-Server-Ms" in h:
                server.append(float(h["X-Server-Ms"][0]))
            cache[(h.get("X-Cache") or ["-"])[0]] += 1
    encode.wait(); attack.wait()
    client.sort(); server.sort()
    n = sum(codes.values())
    ok = codes.get(200, 0)
    duration = args.step_seconds
    s = {
        "rate": rate, "started_at": started, "ended_at": time.time(),
        "requests": n, "ok": ok, "error_rate": (n - ok) / n if n else 1.0,
        "achieved_rps": ok / duration,
        "client_p50": pct(client, 50), "client_p90": pct(client, 90), "client_p99": pct(client, 99),
        "server_p50": pct(server, 50), "server_p90": pct(server, 90), "server_p99": pct(server, 99),
        "codes": {str(k): v for k, v in codes.items()}, "errors": dict(errors.most_common(3)),
        "cache": {k: round(v / ok * 100, 2) for k, v in cache.items()} if ok else {},
    }
    broke = []
    if s["error_rate"] > MAX_ERROR_RATE:
        broke.append(f"errors {s['error_rate']:.1%}")
    if s["achieved_rps"] < MIN_ACHIEVED * rate:
        broke.append(f"served {s['achieved_rps']:.0f}/s")
    s["verdict"] = "OK" if not broke else "BROKE: " + "; ".join(broke)
    (out / f"step_{rate}.json").write_text(json.dumps(s, indent=2))
    return s


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--rates", type=int, nargs="*", default=DEFAULT_RATES)
    ap.add_argument("--step-seconds", type=int, default=60)
    ap.add_argument("--cooldown", type=int, default=20)
    ap.add_argument("--timeout", type=int, default=5)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--keep-going", action="store_true", help="do not stop at the first broken step")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    targets = out / "targets.txt"
    make_targets(targets, args.url.rstrip("/"), 200_000, args.seed)
    print(f"steps {args.rates} x {args.step_seconds}s -> {out}", flush=True)

    rows = []
    hdr = (f"{'rate':>6} {'served':>7} {'err%':>6} | {'client p50':>10} {'p90':>8} {'p99':>8} | "
           f"{'server p50':>10} {'p90':>8} {'p99':>8} | cache / verdict")
    for i, rate in enumerate(args.rates):
        if i:
            time.sleep(args.cooldown)
        print(f"\n--- step {rate}/s for {args.step_seconds}s", flush=True)
        s = run_step(rate, args, targets, out)
        rows.append(s)
        print(hdr)
        print(f"{rate:6d} {s['achieved_rps']:7.0f} {s['error_rate'] * 100:6.2f} | {s['client_p50']:10.1f} "
              f"{s['client_p90']:8.1f} {s['client_p99']:8.1f} | {s['server_p50']:10.2f} {s['server_p90']:8.2f} "
              f"{s['server_p99']:8.2f} | {s['cache']} {s['verdict']} {s['errors'] or ''}", flush=True)
        if s["verdict"] != "OK" and not args.keep_going:
            print("stopping: first broken step", flush=True)
            break

    cols = ["rate", "achieved_rps", "error_rate", "client_p50", "client_p90", "client_p99",
            "server_p50", "server_p90", "server_p99", "verdict", "started_at", "ended_at"]
    with open(out / "steps.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)
    (out / "steps.json").write_text(json.dumps(rows, indent=2))
    print("\n" + hdr)
    for s in rows:
        print(f"{s['rate']:6d} {s['achieved_rps']:7.0f} {s['error_rate'] * 100:6.2f} | {s['client_p50']:10.1f} "
              f"{s['client_p90']:8.1f} {s['client_p99']:8.1f} | {s['server_p50']:10.2f} {s['server_p90']:8.2f} "
              f"{s['server_p99']:8.2f} | {s['verdict']}")
    print("latency in ms")


if __name__ == "__main__":
    main()
