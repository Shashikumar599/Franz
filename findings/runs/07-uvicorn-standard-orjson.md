# Before / after: uvicorn[standard] + orjson Response (2026-11-11)

Only the code changed between the two runs. Normal configuration both times: 1 uvicorn worker, no
API CPU cap, Redis uncapped (effectively: `--cpus 6` = whole VM; see note). Same 60 s warm-up at
100/s, same rates, same seed, vegeta from the Mac, 60 s per step, stop at first broken step
(> 1 % errors or < 95 % served).

- **before** = plain `uvicorn` (h11 + asyncio loop), FastAPI encodes the returned dict.
- **after** = `uvicorn[standard]` (httptools + uvloop, confirmed loaded via /proc/1/maps) and
  `/top-liked` returns `Response(orjson.dumps(body))`.

Machine: Apple M1, 8 cores, 16 GB; Docker VM 6 vCPU / 6.1 GB shared with Kafka ×3, producers,
consumers, Redis, Kafka UI.

## Results per step (served / errors, latency in ms)

| rate | before: served, err | before: server p50 / p99 | before: API CPU | after: served, err | after: server p50 / p99 | after: API CPU |
|---|---|---|---|---|---|---|
| 300 | 300, 0 | 0.67 / 21.6 | 37 % | 300, 0 | 0.53 / 15.5 | 30 % |
| 400 | 400, 0 | 0.60 / 27.4 | 46 % | 400, 0 | 0.48 / 16.1 | 35 % |
| 500 | 500, 0 | 0.57 / 202 | 51 % | 500, 0 | 0.46 / 90.7 | 39 % |
| 600 | 600, 0 | 0.57 / 124 | 59 % | 600, 0 | 0.59 / 354* | 37 % |
| 700 | 700, 0 | 0.51 / 22.2 | 60 % | 700, 0 | 0.41 / 79.3 | 45 % |
| 800 | 800, 0 | 0.49 / 76.6 | 63 % | 800, 0 | 0.37 / 16.7 | 51 % |
| 1000 | 1000, 0 | 0.46 / 109 | 72 % | 1000, 0 | 0.35 / 24.3 | 54 % |
| 1200 | **1022, 14.8 % — broke** | 423 / 4,765 | 111 % | **1200, 0** | 0.34 / 36.3 | 73 % |
| 1500 | – | – | – | **1481, 1.3 % — broke** | 6.9 / 255 | 95 % |

\* after/600: Redis CPU rose to 41 % and consumer lag to 1,274 with a few cache MISS/WAIT during
that minute, a write-side burst unrelated to the code change (likely the producer reconnecting and
catching up). Other steps show Redis steady at 23–25 %.

## Summary

| | before | after | change |
|---|---|---|---|
| **Ceiling (last clean step)** | **1,000 req/s** | **1,200 req/s** (1,481/s served at 1,500 with 1.3 % errors) | **+20 %**, nearly +50 % with a small error rate |
| Marginal API CPU per request (300 → 1000/s) | ~0.50 ms | ~0.34 ms | **−31 %** |
| API CPU at 1000/s | 72 % | 54 % | −25 % |
| Server p50 at 1000/s | 0.46 ms | 0.35 ms | −24 % |
| Server p99 at 1000/s | 109 ms | 24 ms | −78 % |
| Redis CPU | 23–25 % | 23–25 % | unchanged (refresher) |
| Mac idle at the breaking step | 43 % (vegeta 31 %) | 43 % (vegeta 33 %) | – |

## What this shows

1. **The real baseline was much higher than the earlier "~400/s per core".** That figure came from
   Stage B with the API capped at 1 CPU (CFS throttling stalls). Uncapped, the original code served
   1,000/s cleanly on one process. The earlier Python generator (runs 1–3) also hid this: its own
   lag dominated the tail.
2. **The change cut ~31 % of the CPU per request and moved the ceiling from 1,000 to 1,200–1,500/s.**
   Less than the ~60 % the profile pointed at (HTTP stack + encoding), because FastAPI/Starlette
   routing, parameter parsing and the thread hand-off remain in Python, and the API's CPU also
   includes the refresher.
3. **Tail latency improved most** (p99 at 1000/s 109 → 24 ms): less CPU per request means less queueing.
4. **Redis is unaffected and far from limits** (~25 %, almost all refresher merges).
5. Past the ceiling the old code collapsed (14.8 % errors at 1200/s); the new code degraded gently at
   1500/s (1.3 % errors, 99 % served).

## Notes
- Redis had a leftover 1-CPU cap from the stress test when this run started; `docker update --cpus 0`
  does not remove a cap, so it was lifted to `--cpus 6` (the whole VM) at 09:34:33, during the
  before-warm-up and before the first measured step. `before/resources.json` (09:34:12) still shows 1.
- Test data: `before/` and `after/` each hold steps.csv, step_<rate>.json, docker_stats.csv,
  redis.csv, kafka_lag.csv, host.csv, resources.json.
