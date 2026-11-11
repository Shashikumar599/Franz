# System snapshot, 2026-11-11 08:26–08:30 IST (after the Mac slept overnight)

## Verdict
Infrastructure healthy; **the Bluesky leaderboard is empty because the producer is replaying ~6 h of
history after the Mac slept**. Self-healing: catching up at ~18× real time.

| area | state |
|---|---|
| Mac (M1, 8 cores, 16 GB) | 68 % idle, load 3.5, 15 GB used / 1.7 GB compressed / 431 MB free. No tests running. Woke from sleep 08:24. |
| Containers | all 14 up, 0 restarts, no OOM. Kafka brokers ~16 % CPU and ~1.1–1.2 GB RAM each. |
| Kafka quorum | 3 voters, active controller = **node 2** (epoch 64), follower lag 0. Data dir 2.9 GB. |
| Topics | bsky.likes 12p, wiki.edits 6p, wiki.edits.dlq, bsky.likes.dlq; all RF 3, min.insync 2. 0 under-replicated, 0 unavailable. bsky.likes leaders spread 4/4/4. |
| Consumer groups | bsky-likes-leaderboard: 3 members, lag 527 (max 64/partition). wiki-leaderboard: 3 members, lag 0. |
| **Bluesky producer** | **100 % CPU, publishing ~6,950/s, event time 5h20m behind**, catching up ~18 min of backlog per minute. Jetstream drops the connection ~every minute while it replays (2 in 2 min); it resumes from its cursor each time. |
| Wikimedia producer | current (newest edit ~1 s old). |
| Consumers (bsky) | every batch "500 messages: 0 counted": events older than 60 min are skipped by design (outside every window). |
| Redis | 3.3 MB, 0 `bsky:likes:*` buckets (expired overnight, none written since), 65 wiki keys, AOF + RDB ok, 156 clients. |
| API | HTTP 200, ~1–2 ms server time, but **0 items for every window** (no buckets). No errors in 10 min. |
| Test setup still applied | API 4 uvicorn workers, `--cpus 4`; Redis `--cpus 1`. |

## Why
The Docker VM is paused while the Mac sleeps. On wake, the producer reconnects with its saved
Jetstream cursor (~02:30 IST) and Jetstream replays everything since. Kafka absorbs the burst easily
(~7k msg/s, brokers ~16 % CPU); consumers correctly discard likes older than 60 min. The leaderboard
reappears once the replay is within 60 min of now (~14 min after 08:28) and is fully live in ~18 min.

## Lessons
- At-least-once replay after downtime is working as designed: no data lost, Kafka buffered it.
- Consumer lag (527) said "healthy" while the data was 5 h stale: lag measures Kafka → consumer, not
  source → Kafka. End-to-end freshness needs its own signal (now − event time).
- Controller leader epoch 64 and controller now on node 2 (was node 1): repeated sleep/wake cycles
  cause quorum re-elections.
