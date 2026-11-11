"""Redis cache with single-flight: only one process recomputes a missing entry.

Everyone else waits for that result instead of hitting Redis with the same expensive
query. The lock lives in Redis, so this also works across several API instances.
"""
import json
import time
import uuid

# Delete the lock only if it is still ours (it may have expired and been taken by
# someone else). Redis runs the whole script atomically.
RELEASE_LOCK = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class CacheTimeout(Exception):
    """Someone else is computing, but the result did not show up in time."""


class SingleFlightCache:
    def __init__(
        self,
        redis_client,
        ttl_ms,
        lock_ttl_ms=10_000,  # a crashed computer's lock disappears after this
        wait_timeout_s=2.0,  # how long a request waits for another request's result
        poll_interval_s=0.005,  # must be well below ttl_ms, or waiters miss a short-lived entry
    ):
        self._redis = redis_client
        self._ttl_ms = ttl_ms
        self._lock_ttl_ms = lock_ttl_ms
        self._wait_timeout_s = wait_timeout_s
        self._poll_interval_s = poll_interval_s
        self._release_lock = redis_client.register_script(RELEASE_LOCK)

    def get_or_compute(self, name, compute):
        """Return (value, status). Status: HIT (cached), MISS (we computed), WAIT (we waited)."""
        cache_key, lock_key = f"cache:{name}", f"lock:{name}"
        token = uuid.uuid4().hex
        deadline = time.monotonic() + self._wait_timeout_s
        waited = False

        while True:
            value = self._read(cache_key)
            if value is not None:
                return value, "WAIT" if waited else "HIT"

            # Cache is empty: try to become the one request that recomputes.
            # NX = only if the lock does not exist yet; PX = expire on its own.
            if self._redis.set(lock_key, token, nx=True, px=self._lock_ttl_ms):
                try:
                    # Another process may have filled the cache while we raced for the lock.
                    value = self._read(cache_key)
                    if value is not None:
                        return value, "WAIT" if waited else "HIT"
                    value = compute()
                    self._store(cache_key, value)
                    return value, "MISS"
                finally:
                    self._release_lock(keys=[lock_key], args=[token])

            # Someone else holds the lock: wait a moment, then look at the cache again.
            waited = True
            if time.monotonic() >= deadline:
                raise CacheTimeout(name)
            time.sleep(self._poll_interval_s)

    def refresh(self, name, compute):
        """Recompute and store the entry, unless another process is already doing it.

        Used by the background refresher, which runs whether or not the entry is
        missing. Returns True if this call did the work.
        """
        cache_key, lock_key = f"cache:{name}", f"lock:{name}"
        token = uuid.uuid4().hex
        if not self._redis.set(lock_key, token, nx=True, px=self._lock_ttl_ms):
            return False
        try:
            self._store(cache_key, compute())
            return True
        finally:
            self._release_lock(keys=[lock_key], args=[token])

    def _store(self, cache_key, value):
        self._redis.set(cache_key, json.dumps(value), px=self._ttl_ms)

    def _read(self, cache_key):
        raw = self._redis.get(cache_key)
        return json.loads(raw) if raw is not None else None
