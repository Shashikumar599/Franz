# Stage B: ceiling of one API process on one CPU

Setup: API = 1 uvicorn process, `docker update --cpus 1`; Redis `--cpus 1`; refresher 500 ms,
TTL 2 s, LIVE_TOP_K 1000. vegeta from the Mac, open-loop steady rate, 60 s per step, random
queries (minutes 1..60, k 1..20). Server latency = `X-Server-Ms` header (app receives request →
sends headers; includes waiting for a worker thread).

| offered | served | errors | client p50 / p99 | server p50 / p99 | API CPU avg | Redis CPU |
|---|---|---|---|---|---|---|
| 200/s | 200 | 0 | 1.9 / 124 ms | 0.85 / 64 ms | 40 % | 26 % |
| 250/s | 250 | 0 | 1.9 / 498 ms | 0.87 / 213 ms | 39 % | 28 % |
| 300/s | 300 | 0 | 2.1 / 3,071 ms | 0.95 / 2,371 ms | 50 % | 30 % |
| 350/s | 350 | 0 | 1.3 / 116 ms | 0.66 / 55 ms | 43 % | 26 % |
| 400/s | 400 | 0 | 2.3 / 1,889 ms | 1.02 / 1,104 ms | 57 % | 29 % |
| **450/s** | **201** | **55 %** | 253 / 4,950 ms | 185 / 2,476 ms | **97 %** | 28 % |
| 500/s (first run) | 457 | 8.6 % | 193 / 4,861 ms | 132 / 4,674 ms | 83 % | 28 % |

## Findings

1. **Ceiling of one API process on one core: ~400 req/s.** At 450/s it collapses. Redis stays at
   ~28 % throughout: the bottleneck is the Python process, not Redis.
2. **Congestion collapse:** at 450/s offered, only 201/s were served, half of what 400/s served.
   Requests that the client already gave up on (5 s timeout) are still processed by the server (a sync
   endpoint is not cancelled), so CPU goes to answers nobody reads and real throughput falls.
3. **Server time is ~1 ms at the median; the tail is queueing, not work.** p50 stays 0.7–1.0 ms up to
   400/s while p99 jumps to 1–2 s at some steps (300, 400) but not others (350).
4. **The jumpy tail comes from CPU throttling, not average load.** With `--cpus 1`, Linux CFS gives the
   container 100 ms of CPU per 100 ms period. The API runs ~40 request threads + 8 refresher threads;
   when several are runnable at once they use the quota early and the whole container is frozen until
   the next period. Measured: throttled in **779 of 6,240 periods (12.5 %), 19.5 s total**, at an
   average CPU of only 40–57 %. Each freeze lets the queue build, which shows up as p99 spikes.
   Redis (single thread) was throttled in 34 periods only.
5. **CPU per request ≈ 1.4 ms** (57 % of a core at 400/s, refresher included), almost all Python:
   parsing the cached top-100 JSON on every request, building per-item dicts, FastAPI's response
   encoding, and the hand-off to a worker thread. Redis time per request is ~0.05 ms.
6. **Client vs server:** client p50 is ~1 ms above server p50: Docker Desktop port forwarding plus
   vegeta. Small compared with the queueing tail.

## Levers (not applied)
- More processes: uvicorn workers or replicas (Stage C). Note: each worker runs its own refresher.
- Less CPU per request: cache the serialized response per (window, k) or slice without parsing,
  return a pre-encoded `Response`, async endpoint with an async Redis client.
- Shed load instead of collapsing: bound the queue / reject early with 503 when overloaded.
