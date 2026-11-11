# Read-path stress test: resources allocated → scale achieved (2026-11-10)

> **Correction (2026-11-11):** Stages B and C below ran on an **altered system** (CPU caps on the API
> and Redis, and 2/4 uvicorn workers). They are not results for the current system. Measured on the
> normal, unmodified setup (1 worker, no caps), the same code served **1,000 req/s cleanly**: see
> `07-uvicorn-standard-orjson.md`. The "~400 req/s per core" figure is an artifact of the
> 1-CPU cap (CFS throttling).


Goal: not infinite scale, but a measured statement of what this design delivers on the resources
it was given, and which component limits it.

## Machine and allocation

| | |
|---|---|
| Host | MacBook Air, Apple M1, 8 cores (4 performance + 4 efficiency), 16 GB RAM, macOS 26.7 |
| Docker VM | 6 vCPUs, 6.1 GB RAM, shared by everything below |
| Always running in the VM | Kafka ×3 (KRaft), 2 producers, 6 consumers (2 groups ×3), Kafka UI, Redis, API |
| Load generator | vegeta 12.13 on the Mac (outside the VM), open-loop steady rate, random queries (window 1–60 min, k 1–20) |
| Caps for the stress test | Redis `--cpus 1`; API `--cpus` = number of uvicorn workers |
| API settings | refresher pause 500 ms, cache TTL 2 s, live buckets read as top 1000 (`LIVE_TOP_K`) |

"Ceiling" = highest 60 s step served in full with < 1 % errors.

## Results

| config | API CPU cap | ceiling | breaks at | at the ceiling: API CPU / Redis CPU | limiting factor |
|---|---|---|---|---|---|
| 1 worker | 1 CPU | **~400 req/s** (p50 1 ms server-side) | 450/s: served 201/s, 55 % errors | 57 % / 29 % | Python process: ~1.4 ms CPU per request + CPU throttling stalls (12.5 % of periods) |
| 2 workers | 2 CPUs | **~550 req/s** (500 clean, 600 with 0.8 % errors) | 700/s: served 441/s, 37 % errors | 60–122 % of 200 % / 39–49 % | not the CPU cap (≤ 145 % used); in-process contention (GIL, ~50 threads/worker), host contention |
| 4 workers (warm) | 4 CPUs | **~600 req/s** | 700/s: served 84/s, 86 % errors, client port exhaustion | 113–121 % of 400 % / 44–57 % | **the laptop**: 0 % idle, 66 % system CPU, load 26, 8 GB memory compressed, vegeta 115 % CPU |
| Redis alone (`redis-benchmark`, GET of a real 10 KB cache entry) | 1 CPU | ~37,000 GET/s (177,000/s pipelined) | not reached | – | – |

Earlier, uncapped runs (load_test.py, Python generator) served 500/s steady with p50 ~3 ms, but
with the API using up to ~1.6 cores.

## What limits the system, in order

1. **API CPU per request (~1.4 ms of Python).** One worker on one core ≈ 400 req/s. Redis time per
   request is ~0.05 ms: Redis could serve ~90× more reads than one API worker.
2. **Adding workers scales sub-linearly here:** 1 → 2 workers gave +40 %, 2 → 4 gave ~+10 %. The
   caps were never fully used, so the limit moved off the API's allotment and onto the shared machine.
3. **Each worker runs its own refresher.** Merges/s: ~60 (1 worker) → ~137 (2) → ~130–186 (4); Redis
   CPU rose from ~28 % to ~50 % from background work alone. Scaling the API out multiplies this cost
   unless the refresher becomes a single shared job.
4. **Past the ceiling, throughput collapses rather than flattening** (450/s offered → 201/s served;
   700/s → 84/s): timed-out requests are still processed, and the client runs out of ports.
5. **At 4 workers the 16 GB M1 is saturated** (CPU and memory) before the API or Redis is. Further
   scale cannot be measured on this machine.

## Statement

On an 8-core / 16 GB M1 laptop, with Kafka, producers and consumers running alongside, the read path
serves **~400 random leaderboard queries/s per API worker-core** at ~1 ms median server time, and
**~550–600/s in total** before the laptop itself saturates. Redis (1 core) is never the limit for
reads after the top-K merge fix; it has ~50–90× headroom.

Run folders: `stress-*-B-*`, `stress-*-B2-*`, `stress-*-C1-*`, `stress-*-C2-*`, `stress-*-C2b-*`
(each with steps.csv, step_<rate>.json, docker_stats.csv, redis.csv, kafka_lag.csv; C2b also host/).
