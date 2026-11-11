# Write path under a natural replay burst (2026-11-11 08:35–08:50 IST)

After the Mac slept, the Bluesky producer resumed from its ~6 h old Jetstream cursor and replayed
at full speed. From 08:42:32 the replayed likes fell inside the 60-min window, so for ~2.5 min the
whole write path (producer → Kafka → consumers → Redis) ran at many times the live rate.

Configuration note: Redis was still capped at `--cpus 1` and the API had 4 workers from the stress
test. Neither mattered here: Redis used ~4 % CPU, and nobody queried the API (refreshers idle).

## Numbers

| phase | producer → Kafka | consumers read | consumers counted → Redis |
|---|---|---|---|
| replay, older than 60 min (08:35–08:42) | 6,400–11,800/s | 6,700–9,600/s | 0 (skipped by design) |
| **replay inside the window (08:42:44–08:44:47)** | 6,500–11,900/s | 6,200–9,600/s | **6,200–9,600/s (peak 9,600/s)** |
| live again (08:45 onward) | 210–350/s | 245–290/s | 245–290/s |

At the 9,600/s peak:

| | value |
|---|---|
| Redis ZINCRBY | ~8,700/s at 1.2–3.9 µs each |
| Redis EXEC (one MULTI/EXEC per consumer batch) | ~22/s at 0.7–1.7 ms each |
| **Redis CPU** | **~4 %** of one core |
| Kafka consumer lag (bsky-likes-leaderboard) | max 1,553 messages, drained |
| Mac | 55–68 % idle |

## Findings

1. **The write path handled ~9,600 counted likes/s, ~35× the live rate, without strain.** Redis at ~4 %
   CPU and lag peaking at ~1.5k (≈ 0.15 s of traffic) means neither Redis nor the consumers were the
   limit.
2. **Batching is why writes are cheap.** Each consumer writes a batch of up to 500 messages as one
   MULTI/EXEC transaction (~22 transactions/s in total), so Redis does ~2 µs of work per like.
3. **The ceiling was the single Bluesky producer** (one Python process, ~100 % CPU, ~7–12k/s) and the
   upstream replay (Jetstream dropped the connection about once a minute; dips to 0–600/s while it
   reconnected). The producer cannot be scaled out: one upstream feed.
4. **Event-time bucketing behaved correctly during replay:** likes were counted into the minute they
   happened, so the leaderboard's last hour was rebuilt correctly, with no spike in the "current" minute.
5. Replay after downtime = at-least-once in action: no data lost, Kafka buffered ~6 h, the
   consumers' 60-min cutoff discarded what no window can use.

## Compared with reads
Reads: ~400–500 req/s per API process (Python CPU bound), Redis ~25 % (refresher merges).
Writes: ~9,600 likes/s through 3 consumers with Redis at ~4 %. Redis is far from saturation in both.
