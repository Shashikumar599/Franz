"""Per-minute counts in Redis sorted sets.

One sorted set per minute: key = {prefix}{minute}, member = what is ranked, score = count.
"""
import time
from collections import Counter

BUCKET_TTL_SECONDS = 65 * 60  # longest window is 60 min, plus 5 min margin
MAX_EVENT_AGE_SECONDS = 60 * 60  # older events fall outside every window: skip them


def bucket_key(prefix, minute):
    return f"{prefix}{minute}"


class BucketWriter:
    def __init__(self, redis_client, key_prefix):
        self._redis = redis_client
        self._prefix = key_prefix

    def add_batch(self, items):
        """Count (member, event_time_seconds) pairs into minute buckets. Returns how many."""
        now = time.time()
        counts = Counter()  # (minute, member) -> occurrences in this batch
        for member, timestamp in items:
            if now - timestamp > MAX_EVENT_AGE_SECONDS:
                continue
            counts[(timestamp // 60, member)] += 1
        if not counts:
            return 0

        # transaction=True wraps everything in MULTI/EXEC: all commands apply together,
        # so a bucket can never be created without its expiry.
        pipe = self._redis.pipeline(transaction=True)
        for (minute, member), count in counts.items():
            key = bucket_key(self._prefix, minute)
            pipe.zincrby(key, count, member)  # creates the sorted set on first use
            # Absolute expiry from the event's own minute: same result however often it runs.
            pipe.expireat(key, minute * 60 + BUCKET_TTL_SECONDS)
        pipe.execute()
        return sum(counts.values())
