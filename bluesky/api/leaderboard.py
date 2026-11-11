"""Compute the top liked posts for a window by merging per-minute buckets inside Redis."""
import os
import time

KEY_PREFIX = "bsky:likes:"  # must match bluesky/consumer/buckets.py
CACHE_ENTRIES = 100  # top posts kept per window; the `limit` parameter slices these

# The consumer's trimmer cuts a minute bucket to its top 100 only once the minute is closed
# (GRACE_MINUTES = 2 in consumers/core/trimmer.py, run once a minute), so the newest 3 buckets
# can still hold every post liked in that minute: ~12-14k members, ~85 % of them with one like.
# Merging those in full made each window cost ~40k members (~20 ms of Redis).
# Instead, read only their top LIVE_TOP_K, non-destructively (the bucket keeps counting).
# Error bound: a post outside a bucket's top K had at most the K-th score in that minute
# (~2 likes at K=1000, ~10 at K=100, which equals the existing trim of closed buckets).
LIVE_MINUTES = 3
LIVE_TOP_K = int(os.environ.get("LIVE_TOP_K", "1000"))


def window_name(minutes):
    """Cache name for a window length. Shared by the request path and the refresher."""
    return f"top:{minutes}"


def compute_window(redis_client, minutes):
    """Sum the last `minutes` minute buckets (current partial minute included)."""
    started = time.monotonic()
    current_minute = int(time.time()) // 60
    keys = [f"{KEY_PREFIX}{current_minute - i}" for i in range(minutes)]
    tmp_key = f"tmp:top:{minutes}"

    # One transaction: nothing else runs between these commands, so temporary keys never
    # linger and the per-window temp names cannot collide. Missing buckets count as empty.
    pipe = redis_client.pipeline(transaction=True)
    inputs, temps = [], []
    for i, key in enumerate(keys):
        if i < LIVE_MINUTES:
            # ZRANGESTORE ... REV LIMIT: copy only the top K (O(log n + K)), sorted set doing its job.
            live_tmp = f"{tmp_key}:live:{i}"
            pipe.zrangestore(live_tmp, key, 0, LIVE_TOP_K - 1, desc=True)
            inputs.append(live_tmp)
            temps.append(live_tmp)
        else:
            inputs.append(key)  # closed bucket, already trimmed to 100
    pipe.zunionstore(tmp_key, inputs)  # scores of the same post are added up
    pipe.zrevrange(tmp_key, 0, CACHE_ENTRIES - 1, withscores=True)
    pipe.delete(tmp_key, *temps)
    results = pipe.execute()
    rows = results[-2]

    return {
        "computed_at": time.time(),
        "compute_ms": round((time.monotonic() - started) * 1000, 1),
        "items": [{"post": post, "likes": int(score)} for post, score in rows],
    }
