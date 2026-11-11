# Run 3: top-K read of live buckets (LIVE_TOP_K=1000), REFRESH_INTERVAL_MS=500, CACHE_TTL_MS=2000

Same plan and seed as runs 1 and 2 (identical query sequence).

## The fix
`compute_window` merged the newest 3 minute buckets in full. Those are untrimmed (the trimmer only
cuts closed buckets, 2+ min old, once a minute) and hold ~12–14k posts each, ~85 % with one like.
Every window merge read ~40k members to produce a top 100. Now each live bucket contributes only its
top 1000 via `ZRANGESTORE … REV` (non-destructive). Verified on live data before deploying: top 100
identical for 5/15/60-min windows, max undercount 2 likes, merge 18–20 ms → 1.8–2.5 ms.

## Three runs side by side (p50 / p99 in ms)

| test | run 1: 50 ms refresh | run 2: 500 ms, TTL 2 s | **run 3: + top-K fix** |
|---|---|---|---|
| T1 100/s | 40 / 180 | 22 / 164 | **3.4 / 76** |
| T2 200/s | 45 / 195 | 24 / 238 | **2.5 / 77** |
| T3 300/s | 28 / 189 | 35 / 2,428 | **2.8 / 824*** |
| T4 300/s poisson | 28 / 131 | 41 / 3,561 | **3.4 / 627*** |
| T5 bursts 900/s | 60 / 577 | 126 / 1,299 | **4.0 / 991** |
| T6 500/s | 36 / 938 | 132 / 5,068 (13 % timeouts) | **2.8 / 2,414*** (0 errors) |
| T7 bursts 1500/s | 677 / 4,132 (0.1 % err) | 812 / 5,182 (6.9 %) | 408 / 5,539 (10.2 % timeouts)* |

\* load generator flagged as lagging (send-lag p99 27–584 ms).

| | run 1 | run 2 | run 3 |
|---|---|---|---|
| µs per ZUNIONSTORE | 8,000–17,000 | 18,000–24,000 | **1,200–1,900** |
| Redis CPU (one thread) | ~90 % | 69–78 % | **22–27 %** (max 44 %) |
| MISS + WAIT | ≤ 0.4 % | 2–4 % | **≤ 0.3 %** (T2–T7 ≈ 0.0–0.2 %) |
| API CPU avg / max | 15–59 / ≤ 93 % | 15–53 / ≤ 92 % | 26–80 / **162 %** |
| bsky consumer lag max | ~470 | 780 | 471 |

## What it shows

1. **Redis is no longer the bottleneck.** 22–27 % CPU across all loads; merges ~1.2–1.9 ms. The
   refresher runs ~80–90 merges/s (cycles finish faster, so more of them) yet costs a quarter of a core.
2. **Median latency is now ~2.5–4 ms at every steady load up to 500/s** (was 28–45 ms). The 2 s TTL is
   fine again: a window is refreshed every ~0.6–0.8 s.
3. **The tail and the bursts now belong to the API process and the load generator.**
   - API container CPU reached 107–162 % in T4–T7 (one Python process; threads overlap only on I/O).
     In T7 (1500/s bursts) requests queue in the API and 10 % hit the 5 s client timeout.
   - The generator (Python asyncio on the same Mac as Docker) lagged: send-lag p99 27–61 ms in T3–T6,
     584 ms in T7. Its event loop also delays reading responses, so "service time" (from actual send)
     still includes client delay: T6 service p50 2.5 / p99 2,409 ms. The two cannot be separated with
     client-side timing alone.
4. **Kafka lag flat** (≤ 471, baseline ~150–330): consumers unaffected in every test.

## Measurement notes
- Redis PING max of 17–113 ms in T5–T7 is inflated: the recorder's sampler runs inside the API
  container, which was CPU-starved then. Redis CPU (22–44 %) says Redis itself was not slow.
- monitor.py's `kafka-consumer-groups.sh` JVM in kafka-1 still causes kafka-1 CPU spikes every ~15 s.

## Next questions
- True server-side latency: add timing in the API (e.g. a `Server-Timing` header or middleware log)
  so client lag no longer pollutes p99.
- API capacity: more uvicorn workers or replicas; an async endpoint with an async Redis client.
- A load generator that is not starved (more efficient tool, or run it inside the Docker network).
