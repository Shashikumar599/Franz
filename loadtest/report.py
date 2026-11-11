"""Join a load-test run with the metrics recorded during it: one row per test.

Usage:  python loadtest/report.py loadtest/results/<run>
Writes <run>/report.md and prints it.

Test windows come from summary.json (`started_at`/`ended_at`). Runs made before those
fields existed fall back to the requests_<test>.csv file times (written as each test ends).
"""
import csv
import json
import statistics
import sys
from pathlib import Path


def load(path):
    if not path.exists():
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def agg(rows, key, t0, t1, fn):
    vals = [num(r.get(key)) for r in rows if t0 <= float(r["ts"]) <= t1]
    vals = [v for v in vals if v is not None]
    return fn(vals) if vals else None


def fmt(v, d=1):
    return "–" if v is None else f"{v:,.{d}f}"


def main():
    run = Path(sys.argv[1])
    summaries = json.loads((run / "summary.json").read_text())
    redis_rows = [r for r in load(run / "redis.csv") if not str(r.get("cpu_pct", "")).startswith("ERROR")]
    docker = load(run / "docker_stats.csv")
    lag = load(run / "kafka_lag.csv")
    api = load(run / "api_log.csv")

    def container(name_part):
        return [r for r in docker if name_part in r["container"]]
    api_c, redis_c = container("bsky-api"), [r for r in docker if r["container"] == "redis"]
    cons_c = container("bsky-likes-leaderboard")
    bsky_lag = [r for r in lag if r["group"] == "bsky-likes-leaderboard"]

    lines = ["| test | load | got/s | p50 | p99 | max | err % | HIT % | Redis CPU avg / max | API CPU avg / max "
             "| ZUNIONSTORE/s | µs per union | Redis PING p50 max | consumers CPU | bsky lag max | API 503s |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for s in summaries:
        if "started_at" in s:
            t0, t1 = s["started_at"], s["ended_at"]
        else:
            t1 = (run / f"requests_{s['test']}.csv").stat().st_mtime
            t0 = t1 - s["duration_s"]
        cons = None
        if cons_c:   # sum CPU of all consumer replicas per sample time
            by_ts = {}
            for r in cons_c:
                if t0 <= float(r["ts"]) <= t1:
                    by_ts.setdefault(r["ts"], 0.0)
                    by_ts[r["ts"]] += float(r["cpu_pct"])
            cons = statistics.mean(by_ts.values()) if by_ts else None
        lines.append("| " + " | ".join([
            s["test"], f"{s['pattern']} {s['target_rps']:g}/s", fmt(s["achieved_rps"]),
            fmt(s["p50_ms"]), fmt(s["p99_ms"]), fmt(s["max_ms"], 0), fmt(s["error_rate"] * 100, 2),
            fmt(s["cache"].get("HIT")),
            f"{fmt(agg(redis_rows, 'cpu_pct', t0, t1, statistics.mean), 0)} / {fmt(agg(redis_rows, 'cpu_pct', t0, t1, max), 0)}",
            f"{fmt(agg(api_c, 'cpu_pct', t0, t1, statistics.mean), 0)} / {fmt(agg(api_c, 'cpu_pct', t0, t1, max), 0)}",
            fmt(agg(redis_rows, "zunionstore_per_s", t0, t1, statistics.mean)),
            fmt(agg(redis_rows, "zunionstore_usec_per_call", t0, t1, statistics.mean), 0),
            fmt(agg(redis_rows, "ping_p50_ms", t0, t1, max), 2),
            fmt(cons, 0),
            fmt(agg(bsky_lag, "total_lag", t0, t1, max), 0),
            fmt(agg(api, "s503", t0, t1, sum), 0),
        ]) + " |")
    text = "# Load test report: " + run.name + "\n\n" + "\n".join(lines) + "\n\n" + (
        "Latency in ms from the scheduled send time. CPU % is of one core (100 % = one core busy).\n"
        "Redis runs commands on one thread, so Redis CPU near 100 % means it is saturated.\n")
    (run / "report.md").write_text(text)
    print(text)


if __name__ == "__main__":
    main()
