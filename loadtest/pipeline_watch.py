"""Record the Bluesky write path end to end while it runs at high rate (e.g. a replay after downtime).

Every --interval seconds writes one row to <out>/pipeline.csv:
  producer_rps      published/s (from the producer's own `published=` counter in its log)
  behind_s          now - event time of the newest message in bsky.likes (how stale the data is)
  consumed_rps      messages/s read by the bsky-likes-leaderboard consumers (sum of their batch logs)
  counted_rps       of those, likes/s actually written to Redis (inside the 60 min window)
Pair with monitor.py in the same folder for Redis write commands/CPU, Kafka lag and Mac usage.

Stops on its own `--after-caught-up` seconds after the data is live again (behind_s < 10),
or after --max-minutes.

Usage: python loadtest/pipeline_watch.py --out loadtest/results/<run>
"""
import argparse
import csv
import re
import subprocess
import time
from pathlib import Path

PRODUCER = "wikipulse-producer-bluesky-bluesky-producer-1"
CONSUMERS = [f"wikipulse-consumer-bsky-likes-leaderboard-bsky-likes-leaderboard-{i}" for i in (1, 2, 3)]
BATCH = re.compile(r"batch of (\d+) messages: (\d+) counted")


def sh(cmd, timeout=30):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return r.stdout + r.stderr


def published():
    m = re.findall(r"published=(\d+)", sh(["docker", "logs", "--tail", "3", PRODUCER]))
    return int(m[-1]) if m else None


def newest_event_ts():
    out = sh(["docker", "exec", "kafka-1", "/opt/kafka/bin/kafka-console-consumer.sh",
              "--bootstrap-server", "kafka-1:19092", "--topic", "bsky.likes", "--partition", "1",
              "--offset", "latest", "--max-messages", "1", "--timeout-ms", "15000"])
    m = re.search(r'"ts": (\d+)', out)
    return int(m.group(1)) if m else None


def consumer_counts(since, until):
    read = counted = 0
    for c in CONSUMERS:
        text = sh(["docker", "logs", "--since", f"{since:.3f}", "--until", f"{until:.3f}", c])
        for a, b in BATCH.findall(text):
            read += int(a); counted += int(b)
    return read, counted


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=float, default=10)
    ap.add_argument("--after-caught-up", type=float, default=300)
    ap.add_argument("--max-minutes", type=float, default=90)
    args = ap.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    f = open(out / "pipeline.csv", "w", newline="")
    w = csv.writer(f)
    w.writerow(["ts", "producer_rps", "behind_s", "consumed_rps", "counted_rps"])

    start = last = time.time()
    last_pub = published()
    caught_up_at = None
    while time.time() - start < args.max_minutes * 60:
        time.sleep(args.interval)
        now = time.time()
        pub = published()
        read, counted = consumer_counts(last, now)
        ev = newest_event_ts()
        dt = now - last
        behind = round(time.time() - ev) if ev else ""
        prod = round((pub - last_pub) / dt) if pub is not None and last_pub is not None else ""
        w.writerow([f"{now:.1f}", prod, behind, round(read / dt), round(counted / dt)])
        f.flush()
        print(f"{time.strftime('%H:%M:%S')} producer {prod}/s  behind {behind}s  "
              f"consumed {read / dt:.0f}/s  counted {counted / dt:.0f}/s", flush=True)
        last, last_pub = now, pub
        if behind != "" and behind < 10:
            caught_up_at = caught_up_at or now
            if now - caught_up_at >= args.after_caught_up:
                print("caught up; stopping", flush=True)
                break
    f.close()


if __name__ == "__main__":
    main()
