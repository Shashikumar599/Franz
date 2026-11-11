# Profile of the query service request path (2026-11-11)

**How:** throwaway copy of the API (same image, 1 uvicorn process, no CPU cap, port 8001, same
Redis), py-spy 0.4.3 inside it, vegeta at a steady 300 req/s with random queries (window 1–60, k 1–20).
`--gil` = only samples the thread holding Python's GIL, i.e. Python CPU time (waiting on Redis or idle
threads excluded). 1,782 samples over 30 s at 200 Hz. The live API on :8000 was not touched.
Files: `stacks-gil.txt` (collapsed stacks), `06-profile-flame.svg` (open in a browser), plus the first
wall-clock profile `stacks.txt` / `flame.svg` (dominated by waiting, kept for reference).

At 300/s: p50 1.2 ms, p99 17–23 ms, 0 errors.

## Where the Python CPU goes

Request path = 78 % of Python CPU, refresher = 22 %.

| request path, self time | share | lever |
|---|---|---|
| h11: HTTP parsing and response serialization (pure Python) | **28.9 %** | `uvicorn[standard]` → httptools (C parser) |
| asyncio event loop | **16.0 %** | `uvicorn[standard]` → uvloop |
| FastAPI `jsonable_encoder` (walks the returned dict) | **11.0 %** | return a pre-encoded `Response` (or orjson) |
| FastAPI routing, query-param parsing, validation | 8.4 % | inherent to FastAPI |
| uvicorn server/protocol | 5.6 % | – |
| Starlette routing / middleware / response | 5.4 % | – |
| anyio thread hand-off (sync endpoint) | 5.4 % | `async def` endpoint + async Redis |
| stdlib json encode/decode | 4.7 % | mostly `json.dumps` of the response; **`json.loads` of the cached top-100 ≈ 1 %** |
| access log (logging) | 2.8 % | `--no-access-log` (loses visibility) |
| Redis client on the request path (one GET) | small (< 2 %) | – |

Refresher (22 %): 82 % of it is redis-py building commands and parsing replies for the merges.

## Conclusions

1. **About 45 % of request CPU is the HTTP server stack running in pure Python** (h11 + default
   asyncio loop), because `requirements.txt` installs plain `uvicorn` without its C extras.
   `uvicorn[standard]` is a one-line dependency change, no code change.
2. **Response encoding is the next ~15 %** (`jsonable_encoder` + `json.dumps`).
3. **Parsing the cached top-100 is only ~1 %.** Storing the cache as a sorted set and reading k items
   with ZREVRANGE would save almost no CPU. It may still be worth it for design reasons (Redis does
   the slicing, consistent with the top-K fix), but not for throughput.
4. The thread hand-off (5 %) and access log (3 %) are smaller levers.
