# Franz: performance of a real Kafka pipeline on one laptop

Two live public firehoses (every Bluesky like, every English Wikipedia edit) stream through a
3-broker Kafka cluster into Redis and a live leaderboard API. This document records how far the
system goes on fixed hardware, what limited it, and what each optimization bought, with the measured
numbers. Each claim links to the write-up of the test run it comes from (`runs/`); small data files are in `data/`.

## The system

```
Bluesky Jetstream (WebSocket) ─► producer ─► [bsky.likes  12 partitions] ─► group bsky-likes-leaderboard ×3 ─┐
Wikimedia EventStreams (SSE) ─► producer ─► [wiki.edits   6 partitions] ─► group wiki-leaderboard ×3      ─┤
                                              Kafka 4.0, 3 KRaft nodes                                       ▼
                                              RF 3, min.insync.replicas 2              Redis: per-minute sorted sets
                                              producers: acks=all, idempotent                    │
                                                                                                 ▼
                                                   query service (FastAPI): background refresher + single-flight cache
                                                   GET /top-liked?minutes=1..60&limit=1..100
```

- **Key = what is ranked** (post URI, page title): the same item always lands on the same partition, in order.
- **Event-time bucketing:** a like is counted in the minute it happened, not the minute it arrived, so
  replays and delays do not create fake spikes.
- **At-least-once:** consumers commit offsets only after the Redis write.

**Hardware for every number below:** MacBook Air M1 (8 cores: 4 performance + 4 efficiency), 16 GB RAM.
Docker VM with 6 vCPUs / 6.1 GB, shared by all 14 containers (Kafka ×3, 2 producers, 6 consumers,
Redis, query service, UIs). No per-container limits. Load generator on the same Mac.

---

## Headline numbers

| | result |
|---|---|
| **Write path** | **9,600 likes/s** counted end to end (≈ 35× the live rate), Redis at **4 % CPU**, Kafka at ~17 % CPU per broker |
| **Read path** | **1,200 random queries/s** from one API process, **0 errors**, server p50 **0.34 ms**, p99 **36 ms** |
| Read path at 1,000/s | server p99 **24 ms** (was 109 ms) |
| Read-path efficiency | merge cost **20 ms → 1.5 ms**, Redis load **90 % → 25 %**, CPU per request **−31 %** |
| Durability | stop a broker: ISR 3 → 2, writes continue (acks=all, min.insync.replicas 2) |
| Recovery | 6 h of downtime replayed from the producer's saved cursor in ~20 min (at-least-once; nothing skipped by design, not independently counted) |

---

## Write path: 9,600 likes/s

**How it was measured:** after the laptop slept for ~6 h, the Bluesky producer resumed from its saved
Jetstream cursor and replayed the backlog at full speed. Once the replay reached the last 60 minutes,
every replayed like was counted again, a natural, end-to-end write stress test
([05-write-path-replay](runs/05-write-path-replay.md)).

| | live | replay peak |
|---|---|---|
| producer → Kafka | 210–350/s | 7,000–11,900/s |
| consumers counted → Redis | 245–290/s | **9,600/s** |
| Redis ZINCRBY | – | ~8,700/s at ~2 µs each |
| Redis transactions | – | ~22/s (one MULTI/EXEC per 500-message batch) |
| **Redis CPU** | – | **~4 %** |
| Kafka brokers | – | ~17 % CPU each at 7,700 msg/s, every write replicated ×3 |
| consumer lag | ~100–300 | max 1,553, drained |

- **Why it is cheap:** consumers batch up to 500 messages into one Redis transaction, so Redis spends
  ~2 µs per like. Kafka absorbs bursts; consumers read at their own pace from committed offsets.
- **Limit:** the single Bluesky producer (one Python process at 100 % CPU, ~7–12k msg/s). It cannot be
  scaled out: there is one upstream feed, and two producers would duplicate it.
- **Correctness under replay:** event-time bucketing put replayed likes into the minutes they happened, so the current minute did not spike.

---

## Read path: from 40 ms to 0.35 ms, from Redis-bound to 1,200 req/s

Workload for every read test: **open-loop random queries**, window uniform 1–60 min, k uniform 1–20,
so all 60 window lengths stay active (the worst case for the cache refresher).

### 0. Starting point ([01-baseline-refresh-50ms](runs/01-baseline-refresh-50ms.md))
Refresher recomputing all 60 windows every 50 ms.
- p50 **28–45 ms**, p99 130–195 ms at 100–300/s; p99 **938 ms** at 500/s; **4.1 s** under 1,500/s bursts.
- **Redis ~90 % busy** before reads even mattered: ~100 window merges/s at 8–20 ms each.
- Finding: the refresher's cost scales with the number of *distinct windows*, not the request rate.

### 1. Slower refresh: 50 ms → 500 ms ([02-refresh-500ms](runs/02-refresh-500ms.md))
- Merges halved, light-load p50 40 → 22 ms.
- **But it made things worse under load:** one refresh cycle (60 merges × ~20 ms + 500 ms pause)
  outlasted the 2 s cache TTL; 2–4 % of entries expired, waiting requests each held an API thread for
  up to 2 s → **thread-pool starvation, 13 % timeouts at 500/s, zero 503s.**
- Lesson: Redis busy ≈ work / (work + pause), freshness ≈ work + pause. When work dominates, tuning
  the pause moves load, not freshness. The real problem was the work.

### 2. Root cause fixed: read the top-K of live buckets ([03-live-bucket-top-k](runs/03-live-bucket-top-k.md))
- Each merge read the 3 **untrimmed** newest minute buckets in full: ~12–14k posts each, **85 % with a
  single like**, ~40,000 members per merge to produce a top 100.
- Fix: `ZRANGESTORE … REV LIMIT 1000` copies only each live bucket's top 1,000 (non-destructive), then
  `ZUNIONSTORE` merges those with the trimmed buckets. The sorted set does its job at query time.
- Verified on live data: **identical top 100**, worst undercount 2 likes.

| | before | after |
|---|---|---|
| merge time | 18–20 ms | **1.2–1.9 ms** |
| Redis CPU | 70–90 % | **22–27 %** |
| p50 at 100–500/s | 22–45 ms | **2.5–4 ms** |
| MISS + WAIT | 2–4 % | ≤ 0.3 % |

### 3. Measuring correctly
- Open-loop generator (latency from the *scheduled* send time, no coordinated omission); later
  **vegeta** for thousands of req/s.
- Server-side timing: an `X-Server-Ms` header from a plain ASGI middleware separates server time from
  client and port-forwarding delay.
- Traps found and fixed: a Homebrew Redis on `127.0.0.1:6379` shadowed Docker's Redis for host tools; the
  Python generator became the tail-latency bottleneck once the server answered in ~3 ms; CPU caps
  (CFS throttling) distorted results, so the final numbers run on the unmodified setup.

### 4. Profiling: where the CPU really goes ([06-profile](runs/06-profile.md))
py-spy with `--gil` (Python CPU only) at 300 req/s:

| request-path CPU | share |
|---|---|
| HTTP parsing + serialization in **pure Python** (h11) | 29 % |
| asyncio event loop (pure Python) | 16 % |
| FastAPI `jsonable_encoder` + `json.dumps` | ~15 % |
| routing, params, validation | ~14 % |
| parsing the cached top-100 (`json.loads`) | **~1 %** |

About **45 % of each request was the web server running in pure Python**, because plain `uvicorn` was
installed without its C extensions.

### 5. C HTTP stack + direct encoding ([07-uvicorn-standard-orjson](runs/07-uvicorn-standard-orjson.md))
- `uvicorn[standard]`: **httptools** (Node.js's C parser, llhttp) + **uvloop** (libuv event loop).
- `/top-liked` returns `Response(orjson.dumps(body))`: FastAPI's two Python encoding passes are skipped.
- Same machine, same normal setup (1 worker, no caps), same seed, only the code changed:

| | before | after |
|---|---|---|
| **clean ceiling (60 s steps, 0 errors)** | **1,000 req/s** | **1,200 req/s** |
| at 1,200/s | 1,022 served, 14.8 % errors | 1,200 served, 0 errors |
| at 1,500/s | – | 1,481 served (99 %), 1.3 % errors |
| API CPU per extra request | ~0.50 ms | **~0.34 ms (−31 %)** |
| server p50 / p99 at 1,000/s | 0.46 / 109 ms | **0.35 / 24 ms** |

---

## Optimization ledger

| # | change | type | effect |
|---|---|---|---|
| 1 | refresher pause 50 → 500 ms | config | merges halved; exposed TTL / thread-starvation failure mode |
| 2 | **top-K read of live buckets** (`ZRANGESTORE … REV`) | query | merge 20 → 1.5 ms, Redis 90 → 25 %, p50 40 → 3 ms |
| 3 | server timing header + open-loop vegeta | measurement | separated server time from client noise |
| 4 | **`uvicorn[standard]`** (httptools + uvloop) | dependency | biggest CPU lever (~45 % of request CPU was pure-Python HTTP) |
| 5 | **direct `orjson` Response** | code | skip FastAPI's encoding passes |
| – | 4 + 5 together | | ceiling 1,000 → 1,200 req/s, CPU/request −31 %, p99 109 → 24 ms |

## Lessons worth keeping

- **Consumer lag is not freshness.** Twice the data was minutes to hours stale (producer behind its
  source) while consumer lag looked healthy: lag only measures Kafka → consumer.
- **Kafka decouples failure.** Redis at 90 % slowed consumers; producers never noticed. A 6 h outage
  replayed from the saved cursor.
- **Overload collapses, it does not flatten.** Past the ceiling, timed-out requests still consume
  server CPU and useful throughput falls (old code: 14.8 % errors at 1,200/s; new code degrades gently).
- **Profile before optimizing.** The intuitive fix (store the cache as a sorted set to avoid parsing
  100 items) would have saved ~1 %. The real cost was the HTTP stack.
- **Measure the measurer:** shadowed ports, client lag, CPU caps and observer effects all produced
  wrong numbers at some point before they were caught.

## Not yet done
Sustained runs (20+ min) at 1,000–1,200/s; Poisson and burst patterns on the current code; controller
failover and two-broker failure; horizontal scaling of the query service (needs a single shared refresher).
