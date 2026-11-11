# WikiPulse — Implementation Steps

Working checklist of the project phases. Decisions are recorded here once made.

## Decisions log

| Decision | Choice | Date |
|---|---|---|
| Language / Kafka client | Python + `confluent-kafka` | 2026-11-09 |
| Repo layout | One folder per component (`producer/`, `consumer/`; `api/`, `dashboard/` later); one root `.venv` + `requirements.txt` | 2026-11-09 |
| Kafka image (Apache vs Confluent vs Bitnami) | _open_ | |
| Kafka image / version | `apache/kafka:4.0.0` (Kafka 4.x, KRaft only) | 2026-11-09 |
| Redis | Own compose file `redis/docker-compose.yml` (Redis 7 + RedisInsight UI on 5540), on Kafka's network. AOF persistence with `appendfsync everysec` (supersedes "disposable, no persistence"). Window aggregation runs inside Redis (`ZUNIONSTORE`) | 2026-11-10 |
| Producers | One package/image `producers/`: `StreamProducer` ABC (`connect()` reads one connection, `to_message()` maps/filters), shared core (reconnect+backoff in the base class, run loop, Kafka publisher, ack tracker, checkpoint), two subclasses `WikimediaProducer` and `BlueskyLikesProducer`. Runs as containers, one compose file per source (`producers/deploy/wikimedia.yml`, `bluesky.yml`; same image, different `command`; own compose project `name:`), checkpoints on a named volume per source, SIGTERM flushes and saves. Replaces `producer/` and `bluesky/producer/` |
| Consumers | Same pattern: `consumers/` with `StreamConsumer` ABC (`handle_batch()`, optional `maintenance()`), Kafka loop in `core/runner.py`, `LeaderboardConsumer` (Redis minute buckets + optional trimming) with two small groups `wiki-leaderboard` and `bsky-likes-leaderboard`; one compose file per group in `consumers/deploy/`. Wiki group id renamed `leaderboard` -> `wiki-leaderboard`. Replaces `consumer/` and `bluesky/consumer/` | 2026-11-10 |
| Restart policy | `restart: unless-stopped` on every service in every compose file | 2026-11-10 |
| Tests | pytest unit tests: `producers/tests` (AckTracker, both `to_message`, SSE parsing), `consumers/tests` (to_count, bucket write, trimming with a fake Redis). Run each package separately: `python -m pytest producers`, `python -m pytest consumers` (both define a `core` package) | 2026-11-10 | 2026-11-10 |
| Deployment split | Root `docker-compose.yml` stays Kafka + UI. Every producer and every consumer group has its own compose file (`producers/deploy/`, `consumers/deploy/`), joining Kafka's network `wikipulse_default` as an external network | 2026-11-10 |
| Broker count / replication | Started with 1 broker, RF 1. **Now 3 KRaft nodes (combined broker+controller), RF 3, `min.insync.replicas=2`, named volume per broker** (data survives `down`, not `down -v`). Clients use a bootstrap list of all three brokers (`kafka-1..3:19092` in Docker, `localhost:9092,9093,9094` on the host) | 2026-11-10 |
| Kafka UI | `kafbat/kafka-ui` (maintained fork) | 2026-11-09 |
| `wiki.edits` partitions | _open_ (proposal: 6) | |
| Redis member | Was `wiki:title` (for all wikis); with enwiki only the prefix is redundant. To reconfirm at step 3.4 | 2026-11-09 |
| Offset commits | Manual commit after processing, from the start (Phase 6 then focuses on crash/replay experiments, not on switching) | 2026-11-09 |
| Bot edits | Included (no bot filter; data volume is low) | 2026-11-10 |
| Wikis counted | **`enwiki` only** (same page has different titles per language, so mixing languages duplicates subjects). Supersedes "all wikis" and the wikidata/commons exclusion. Cross-language merge via Wikidata IDs is an optional later phase | 2026-11-10 |
| Filters (bots / types / wikis) | **Filter in the producer** (`producer/filters.py`): only `enwiki` `edit`/`new` events are published; nothing we won't process is stored. Supersedes the earlier "publish everything, filter in consumer". Trade-off accepted: dropped events cannot be recovered from Kafka | 2026-11-10 |
| Consumer design | Two consumer groups on `wiki.edits`: leaderboard (top-k) + a second job (TBD) | 2026-11-09 |
| Dashboard tech | _open_ | |

## Measured facts

- Unfiltered recentchange rate into `wiki.edits`: **~37 events/s** (2,239 in 60 s, measured 2026-11-09 via summed partition end offsets) ≈ 134k events/hour.
- Live rate re-measured: **~31 events/s** (955 in 30 s), so live traffic is 31–37 events/s depending on the time of day.
- A second measurement gave ~4,400 events/s (132,187 in 30 s). Confirmed NOT the live rate: the producer was restarted after being stopped and resumed from its checkpoint, so Wikimedia replayed history at full speed. It does show the pipeline (producer `acks=all` + idempotence, 1 broker, consumer with batch commits) sustaining thousands of events/s.
- Keys (page titles) spread evenly: 2,345 messages over 6 partitions gave 334–480 per partition.

## Bluesky track (`bluesky/`): top-liked posts from Jetstream

Added because Wikimedia `enwiki` edits are under 1 event/s, too slow to exercise Kafka scaling. Wikimedia code is left as is.

- Source: Bluesky Jetstream (WebSocket). Likes only (`collections=app.bsky.feed.like&kinds=commit`). Endpoint/format taken from the v2 docs page, to be verified with the probe (`python bluesky/producer/jetstream_reader.py`).
- Topic `bsky.likes`, **key = liked post's AT URI** (all likes of a post land on one partition). Created by Shashi.
- Producer (`producers/sources/bluesky.py`): extract like, publish `{post, liker, ts}`; resume from Jetstream `seq` via the shared ack-watermark + checkpoint core.
- Consumer group `bsky-likes-leaderboard` (`consumers/groups/bsky_likes_leaderboard.py`, deployed by `consumers/deploy/bsky-likes-leaderboard.yml`): per-minute sorted sets `bsky:likes:{minute}`, member = post URI, same transactional write, `EXPIREAT`, commit after the Redis write.
- Known gaps: unlikes (`delete`) carry no post URI so they are not subtracted; post text is not in the like event (enrichment would need the Bluesky API); at-least-once duplicates until dedup; Jetstream cursor replay window length unknown.
- Query service (`bluesky/api/`, FastAPI, host port 8000): `GET /top-liked?minutes=1..60&limit=1..100`. Window = current partial minute + previous N-1, merged inside Redis (`ZUNIONSTORE` in one transaction). Cache `cache:top:{minutes}` holds the top 100, `limit` slices it; TTL 50 ms (`CACHE_TTL_MS`, Shashi's choice). Single-flight: `SET lock:top:{minutes} <token> NX PX 10000`; one request computes, others poll every 5 ms for up to 2 s, then 503. Lock released by compare-and-delete Lua. Response header `X-Cache: HIT|MISS|WAIT`.
- UI: plain HTML/JS served by the same app at `/` (window buttons, auto-refresh, cache status badge). Plain HTML chosen by Claude since Shashi said "go and build"; fills the "dashboard tech" open decision for this track.
- Risk flagged: 50 ms TTL is shorter than a 60-bucket union (~240k members) is likely to take, so the cache may barely reduce load; measure `compute_ms` first.
- Trimming (`consumers/core/trimmer.py`, enabled per group with `trim_keep`; on for bsky only): once a minute, from the consumer's main loop (no threads), every closed bucket (minutes `now-60` to `now-2`) is cut to its top 100 with `ZREMRANGEBYRANK key 0 -101`. Live and previous bucket are never trimmed. Idempotent, so every replica can run it. Makes window sums approximate: a post outside a minute's top 100 loses that minute's likes. Purpose: bound the window union to a few thousand members so a 50 ms cache TTL is workable.
- Background refresher (`bluesky/api/refresher.py`): every window a request has asked for in the last 60 s is recomputed in a loop, all active windows in parallel on a thread pool (8 workers), pause 50 ms between cycles (`REFRESH_INTERVAL_MS`). Uses the same Redis lock (`SingleFlightCache.refresh`), so several API instances do not duplicate work. Requests normally HIT; a cold window falls back to the single-flight compute. `CACHE_TTL_MS` is now a safety expiry (2000 ms): staleness is about compute time + refresh interval, not the TTL. This replaces Shashi's 50 ms TTL, which could not keep entries alive between refreshes.
- Caveat: Redis runs `ZUNIONSTORE` serially, so refresher threads overlap network and Python time but not Redis CPU.
- Not yet: metadata.

## Open questions (to answer later)

- **Topic retention.** Kafka's default is 7 days. At ~37 events/s and roughly 1.3 KB per message (3 MB / 2,345 in the UI, approximate), that is about 175 MB/hour, 4 GB/day, ~29 GB per week unfiltered. Decide a `retention.ms` (the leaderboard only needs 1 h, replay experiments may want more).

- **Producer resume strategy.** Currently an ack watermark (newest event id where it and all earlier events are acked) saved to `producer/checkpoint.json`. Why not just "last acked id": acks can arrive out of order across partitions/retries, so event 7 may be acked while 5 is in flight, and a crash would skip 5. Options to decide between: (a) keep the watermark, (b) simpler "last acked id" (can lose events), (c) no checkpoint, resume from "now" (gap on restart). Related: replay duplicates events (needs consumer dedup on `meta.id`, Phase 8); Wikimedia history is only 7–31 days.
- Producer settings (`acks`, idempotence, `linger.ms`) are placeholders until step 2.3.
- Partition count 6 for `wiki.edits` is the plan's proposal, not formally confirmed.

## Phase 0 — Repo setup
- [x] Decide language + client library
- [x] Decide repo layout
- [x] Create skeleton + README with project goal (`.gitignore`, `requirements.txt`, `producer/`, `consumer/`)
- [x] Shashi creates venv and installs requirements (import verified)

## Phase 1 — Kafka cluster (current focus: discussion)
- [x] 1.1 Choose Kafka image and version (`apache/kafka:4.0.0`)
- [x] 1.2 Design listeners (INTERNAL `kafka:19092`, EXTERNAL `localhost:9092`, CONTROLLER 9093)
- [x] 1.3 Write `docker-compose.yml`: single KRaft node (broker+controller) — 3 nodes deferred to Phase 7
- [x] 1.4 Add Kafka UI service (host port 8081)
- [x] 1.5 Start cluster; broker registered
- [x] 1.6 Create `wiki.edits` and `wiki.edits.dlq` via CLI
- [x] 1.7 Describe topics; read leader/replicas/ISR per partition (all leader 1, ISR 1)
- [x] 1.8 Identify the active controller (`kafka-metadata-quorum`): node 1, sole voter
- [x] 1.9 Console produce/consume smoke test (`demo` topic, keys → partitions observed)
- **Done when:** cluster up, topics visible in UI and CLI, active controller identified.

## Phase 2 — Producer
- [x] Decide SSE approach: `requests` with `stream=True`, parse `data:` lines by hand
- [x] 2.1 `producer/stream_reader.py`: read the SSE stream and print events (no Kafka yet)
- [ ] 2.2 Publish to `wiki.edits` (key = title); track acked event id via watermark; checkpoint file for resume (`ack_tracker.py`, `checkpoint.py`, `publisher.py`, `run_producer.py`)
- [ ] Parse + publish to `wiki.edits`, key = `title`
- [ ] Settings discussion: `acks`, idempotence, retries, `linger.ms`, batching
- [ ] Verify same title → same partition in UI

## Phase 3 — Leaderboard consumer + Redis
- [ ] 3.1 `consumer/run_consumer.py`: group `leaderboard`, read + print + manual commit (no Redis, no filters)
- [x] 3.2 Filters moved to the producer (`producer/filters.py`): `enwiki` only, bots included, `type` in edit/new (type filter not explicitly confirmed). Skipped events advance the checkpoint via `AckTracker.skip()`
- [ ] 3.3 Redis + RedisInsight in `redis/docker-compose.yml` (written; to be verified); Python client `redis-py` still to add
- [ ] 3.4 Per-minute buckets `wiki:title:{minute}` (event time), member = page title, written in one MULTI/EXEC transaction per batch (`ZINCRBY` + `EXPIREAT minute*60+3900`), events older than 60 min skipped, commit after the Redis write (`consumer/buckets.py`; written, to be verified)
- [ ] 3.5 Page metadata hash per title written in the same transaction (fields to be decided by Shashi)
- [ ] 3.6 Trim closed buckets to the top 100 (`ZREMRANGEBYRANK key 0 -101`) from a ~1 min scheduler in the consumer (Shashi said "producer"; read as consumer since it holds the Redis client). Never trim the live bucket (undercounts titles that later climb). Trimming makes window sums approximate
- [ ] 3.7 Manual window query: `ZUNIONSTORE` over the last N buckets + `ZREVRANGE 0 9 WITHSCORES` returns a sensible top 10
- [ ] Manual Redis query returns sensible top 10

## Phase 4 — API + dashboard
- [ ] `GET /top?window=5|10|30|60` (ZUNIONSTORE + ZREVRANGE)
- [ ] Dashboard with window buttons + auto-refresh

## Phase 5 — Consumer group experiments
- [x] 5.0 Consumer containerized (`consumer/Dockerfile`, `consumer` service in compose, bootstrap via `KAFKA_BOOTSTRAP_SERVERS`, SIGTERM handled); scale with `docker compose up -d --scale consumer=N` (base image `python:3.12-slim`, not discussed)
- [ ] 1 → 3 → 8 instances; observe assignment / idle consumers
- [ ] Kill instance; observe rebalance
- [ ] Second consumer group gets all messages

## Phase 6 — Offsets and delivery semantics
- [ ] Manual commit after Redis write
- [ ] Crash before commit → reprocessing
- [ ] Reset offsets to earliest; wipe Redis; rebuild
- [ ] Slow consumer → lag grows and drains

## Phase 7 — Durability and broker failure
- [ ] Stop partition leader; observe election + ISR
- [ ] `acks=0/1/all` × `min.insync.replicas` while killing brokers
- [ ] Stop active controller; observe failover

## Phase 8 — Correctness
- [ ] Event-time bucketing check (stop consumer 10 min)
- [ ] Dedup via `meta.id` (`SET NX` + TTL)
- [ ] DLQ for malformed events

## Phase 9 — Kubernetes (optional)
- [ ] kind/k3d + Strimzi, KEDA on lag

## Phase 10 — Write-up
- [ ] README: architecture, how to run, experiments, Kafka vs NATS JetStream
