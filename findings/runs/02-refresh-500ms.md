# Run 2: REFRESH_INTERVAL_MS=500, CACHE_TTL_MS=2000 (compare with full-20261110-192906)

Same plan and seed as run 1 (identical query sequence). Recorder sampled the correct (Docker) Redis
for the whole run.

| test | run 1 (50 ms) p50 / p99 | run 2 (500 ms) p50 / p99 | run 2 MISS+WAIT | run 2 errors |
|---|---|---|---|---|
| T1 100/s | 40 / 180 ms | **22 / 164 ms** | 0.7 % | 0 |
| T2 200/s | 45 / 195 ms | 24 / 238 ms | 0.8 % | 0 |
| T3 300/s | 28 / 189 ms | 35 / 2,428 ms | 2.6 % | 0 |
| T4 300/s poisson | 28 / 131 ms | 41 / 3,561 ms | 3.1 % | 0 |
| T5 bursts 900/s | 60 / 577 ms | 126 / 1,299 ms | 2.4 % | 0 |
| T6 500/s | 36 / 938 ms | 132 / 5,068 ms | 3.6 % | **13.1 % (11,785 timeouts)** |
| T7 bursts 1500/s | 677 / 4,132 ms | 812 / 5,182 ms | 4.4 % | 6.9 % |

| | run 1 | run 2 |
|---|---|---|
| ZUNIONSTORE/s | 50–100 | 30–36 |
| µs per union | 8,000–17,000 | 18,000–24,000 |
| Redis CPU | ~90 % | 69–78 % |
| bsky consumer lag max | ~470 | 780 (T7) |

## What happened

1. **Fewer merges, as intended** (~32/s instead of 50–100/s), and light-load latency improved
   (T1 p50 22 ms vs 40 ms). Redis CPU fell less than expected (to ~70 %, not ~45–50 %) because each
   merge cost ~20 ms instead of ~8 ms. Merge cost follows the bucket sizes (data), not the request rate.
2. **The 2 s TTL is shorter than the gap between refreshes.** Cycle ≈ 60 merges × ~20 ms (≈ 1.2 s,
   serialized in Redis) + 500 ms pause; one window can wait pause + 2 × work ≈ 2.9 s or more between
   refreshes. Its cache entry expires first, so 2–4 % of requests became MISS (compute it themselves:
   more Redis load) or WAIT (poll every 5 ms for up to 2 s).
3. **WAITs starve the API's threads → timeouts.** `/top-liked` is a sync endpoint, so each request
   occupies one worker thread of a small pool (~40 by default) for its whole duration. A WAIT holds its
   thread for up to 2 s while polling. With 3 % of 500/s waiting ~1–2 s, that is ~15–30 threads
   permanently busy waiting; new requests queue behind them and hit the client's 5 s timeout.
   That is the 11,785 timeouts in T6, with **zero 503s from the API** (requests never got a thread).
4. **Kafka lag rose for the first time** (to ~780 in T7, baseline ~150–300): Redis busy enough that
   the consumers' writes slowed. Still small (~4 s of traffic) and it drained.

## Takeaways

- The refresh pause sets Redis busy ≈ work / (work + pause), while freshness ≈ work + pause. With 60
  windows the work (~1.2 s) dominates, so the pause mostly moves Redis load, not freshness.
- The TTL must exceed pause + 2 × work with margin: ~5 s here. Freshness comes from the refresher;
  the TTL is only the fallback if the refresher dies.
- Single-flight waiting inside a sync thread pool turns a small miss rate into thread starvation.
  A cache miss is cheap; a waiting thread is expensive.

Next: REFRESH_INTERVAL_MS=500, CACHE_TTL_MS=5000, same plan. Expect MISS+WAIT ≈ 0.1 %, no timeouts.
Client lag p99 was 26 ms (T6) and 60 ms (T7): the generator also struggled once the server stalled.
