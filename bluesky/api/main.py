"""Query service: top liked Bluesky posts in the last N minutes.

Run on the host:  uvicorn main:app --port 8000   (from bluesky/api/)
UI at /, API docs at /docs.

A background refresher keeps every recently requested window in the cache, so requests
normally just read it. A request computes only on a cold cache, guarded by a lock.
"""
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

import orjson
import redis
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import FileResponse

from cache import CacheTimeout, SingleFlightCache
from leaderboard import compute_window, window_name
from refresher import Refresher

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
# Entries only have to outlive one refresh cycle; the refresher keeps them fresh.
# If the refresher stops, they expire and requests fall back to computing themselves.
CACHE_TTL_MS = int(os.environ.get("CACHE_TTL_MS", "2000"))
REFRESH_INTERVAL_MS = int(os.environ.get("REFRESH_INTERVAL_MS", "500"))  # pause between cycles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
cache = SingleFlightCache(redis_client, ttl_ms=CACHE_TTL_MS)
refresher = Refresher(
    cache,
    compute=lambda minutes: compute_window(redis_client, minutes),
    interval_s=REFRESH_INTERVAL_MS / 1000,
)


@asynccontextmanager
async def lifespan(app):
    refresher.start()
    yield
    refresher.stop()


app = FastAPI(title="Bluesky top liked posts", lifespan=lifespan)


class ServerTimingMiddleware:
    """Adds `X-Server-Ms`: time from the app receiving the request to sending the response
    headers. Includes waiting for a worker thread and the Redis calls; excludes the socket
    backlog, port forwarding and the client. Lets load tests separate server time from
    client-side delay. Plain ASGI (not BaseHTTPMiddleware) to keep its own overhead tiny."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        started = time.perf_counter()

        async def send_with_timing(message):
            if message["type"] == "http.response.start":
                ms = f"{(time.perf_counter() - started) * 1000:.2f}".encode()
                message["headers"] = list(message.get("headers", [])) + [(b"x-server-ms", ms)]
            await send(message)

        await self.app(scope, receive, send_with_timing)


app.add_middleware(ServerTimingMiddleware)


def describe_post(rank, item):
    """Split an AT URI (at://<did>/app.bsky.feed.post/<rkey>) and build a bsky.app link."""
    did, _collection, rkey = item["post"].removeprefix("at://").split("/")
    return {
        "rank": rank,
        "likes": item["likes"],
        "did": did,
        "rkey": rkey,
        "url": f"https://bsky.app/profile/{did}/post/{rkey}",
    }


@app.get("/top-liked")
def top_liked(
    minutes: int = Query(15, ge=1, le=60, description="window length in minutes"),
    limit: int = Query(10, ge=1, le=100),
):
    refresher.touch(minutes)  # keep this window warm from now on
    try:
        # One cache entry per window length; it holds the top 100, `limit` slices it.
        result, status = cache.get_or_compute(
            window_name(minutes), lambda: compute_window(redis_client, minutes)
        )
    except CacheTimeout:
        raise HTTPException(503, "result is being computed, retry", headers={"Retry-After": "1"})
    except redis.RedisError as exc:
        raise HTTPException(503, f"redis unavailable: {exc}")

    body = {
        "minutes": minutes,
        "limit": limit,
        "source": status,  # HIT | MISS | WAIT
        "computed_at": result["computed_at"],
        "compute_ms": result["compute_ms"],
        "items": [describe_post(i, item) for i, item in enumerate(result["items"][:limit], 1)],
    }
    # Hot path: encode the JSON ourselves and return a Response. FastAPI then skips its own
    # jsonable_encoder + json.dumps (two Python passes over the data, ~15 % of request CPU in the
    # profile). Trade-off: no response validation and /docs shows no response schema (there was none).
    return Response(content=orjson.dumps(body), media_type="application/json",
                    headers={"X-Cache": status})


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")
