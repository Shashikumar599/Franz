# Read-path load test: findings (run full-20261110-192906)

Setup: one `bsky-api` container (single uvicorn process), Redis 7.4 (Docker), refresher at
`REFRESH_INTERVAL_MS=50`, `CACHE_TTL_MS=2000`. Random queries: minutes 1..60, k 1..20.
Each test 3 min, open-loop, latency measured from the scheduled send time.

## Results

| test | load | p50 | p99 | p99.9 | max | errors | verdict |
|---|---|---|---|---|---|---|---|
| T1 | uniform 100/s | 40 ms | 180 ms | 800 ms | 1.0 s | 0 | PASS |
| T2 | uniform 200/s | 45 ms | 195 ms | 436 ms | 0.8 s | 0 | PASS |
| T3 | uniform 300/s | 28 ms | 189 ms | 604 ms | 0.8 s | 0 | PASS |
| T4 | poisson 300/s | 28 ms | 131 ms | 331 ms | 0.4 s | 0 | PASS |
| T5 | burst 300/s avg (900/s peaks) | 60 ms | 577 ms | 962 ms | 1.1 s | 0 | FAIL (p99) |
| T6 | uniform 500/s | 36 ms | 938 ms | 1.1 s | 1.2 s | 0 | FAIL (p99) |
| T7 | burst 500/s avg (1500/s peaks) | 677 ms | 4.1 s | 5.2 s | 5.4 s | 96 (0.11 %) | FAIL |

Throughput was always met (every test served its full target rate); what degrades is latency.
No HTTP 503s at any point. T7 errors: 94 client timeouts (5 s) and 2 connection errors, all in
one ~10 s window at t≈110–120 s, when in-flight requests rose to 607.

## What was under load

| | Redis CPU (one thread) | ZUNIONSTORE/s | µs per union | API CPU avg / max |
|---|---|---|---|---|
| T5 | 91 % | 62 | 13,900 | 33 / 80 % |
| T6 | 91 % | 57 | 15,000 | 46 / 61 % |
| T7 | 89 % | 49 | 17,300 | 59 / 93 % |

(Redis numbers exist only from T4 onward; see "Measurement issues".)

1. **Redis is ~90 % busy before reads matter, because of the refresher, not the requests.**
   Random windows keep all 60 window lengths "active", so the refresher recomputes 60 windows per
   cycle. ~50–100 ZUNIONSTORE/s at 8–20 ms each ≈ 0.9 s of Redis's single thread per second.
   Requests themselves are cheap cache GETs (HIT ≥ 99.5 % in every test).
2. **Reads wait behind merges.** Redis runs one command at a time; a GET that arrives during a
   15–30 ms ZUNIONSTORE waits for it. This is the tail (p99/p99.9 of 0.2–1 s) even at 100/s.
3. **Merges got slower as load rose: 8 ms → 14 → 15 → 17 ms (31 ms at the T7 stall).** Measured
   inside Redis, so it is execution time, not queueing. Likely causes (not yet separated): the
   newest untrimmed minute buckets growing during the minute, and CPU contention in the Docker VM
   (6 vCPUs shared by Redis, API, Kafka x3, consumers).
4. **Bursts break the API's queue.** At 1500/s peaks the single API process (Python, one GIL)
   cannot keep up; requests queue during each 1 s burst and the backlog never fully drains in the
   4 s gap (p50 stays 300–800 ms). API CPU peaked at 93 % ≈ one core.
5. **T7 stall at t≈110–120 s:** union time rose to 31 ms and Redis ops/s dropped, API CPU *fell*
   to 39 % while its memory rose 118 → 161 MiB and in-flight requests hit 607. Reading: API worker
   threads blocked waiting on Redis, requests piled up behind them, 5 s timeouts followed. Redis
   takes an RDB snapshot every ~61 s ("10000 changes in 60 seconds. Saving...") but none fell in
   the stall (14:22:52–14:23:07 UTC; snapshots at 14:22:24 and 14:23:25), so that is ruled out.
   Side note: RDB snapshots run every minute on top of AOF; worth knowing, not a problem here.
6. **Kafka lag did not move.** bsky-likes-leaderboard total lag stayed 70–470 in every test, the
   same as the idle baseline (~0.5–1 s of traffic). The consumers' writes (one MULTI/EXEC per
   batch) still got through a 90 %-busy Redis. Kafka never had to absorb a backlog in this run.

## Capacity, as of this configuration

- Steady random reads: ~300/s with p99 < 200 ms. 500/s steady is served but p99 ≈ 1 s.
- Bursty traffic: 900/s peaks push p99 to ~0.6 s; 1500/s peaks overload it.
- First bottleneck: Redis's single thread, consumed by the refresher. Second: the single API process
  under bursts.

## Measurement issues (read before trusting numbers)

- **Wrong Redis for T1–T3.** A Homebrew Redis 8.10 on the Mac listens on 127.0.0.1:6379 and
  [::1]:6379 and shadows Docker's port forward, so the recorder first sampled that idle instance.
  Its data is kept as `redis_HOMEBREW_wrong_instance.csv`. `redis.csv` is the correct Docker Redis,
  sampled from inside the API container, starting at T4. monitor.py is fixed for future runs.
- **Observer effect:** kafka-1 CPU spikes to 150–180 % every ~15 s come from monitor.py's own
  `kafka-consumer-groups.sh` (a JVM started inside kafka-1), not from load.
- **Load generator:** client-lag p99 was ≤ 2 ms in T1–T6, but 36 ms in T7 (up to 96 ms in some
  10 s windows), so part of T7's latency is the generator on the same Mac. The server-side picture
  (in-flight 607, timeouts) is still real.
- Test windows in report.md were derived from file times (± a few seconds); load_test.py now
  records exact start/end times.

## Open questions for the next run

- How much of the p50 (~30–45 ms even at 100/s) is Docker Desktop port forwarding and the API's
  own threads versus Redis queueing? Compare: same load with only one window active
  (e.g. `--minutes-fixed 15`, needs a small script change).
- Effect of `REFRESH_INTERVAL_MS=500`, `CACHE_TTL_MS=2000` (Shashi's proposal): expect ~60
  unions/s → ~45–50 % Redis CPU. Add a result-age column (now − `computed_at`) to measure the
  freshness cost.
- Does Kafka lag ever grow? Needs Redis fully saturated (e.g. many API replicas or a heavier read mix).
