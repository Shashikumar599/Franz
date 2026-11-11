"""Keep only the top members of each closed minute bucket, so window queries stay cheap.

A window query merges up to 60 buckets. Trimming each closed bucket to its top members
bounds that merge to a few thousand entries instead of hundreds of thousands. The cost:
window totals become approximate (a member outside a minute's top N loses that minute).
"""
import logging
import time

from core.buckets import bucket_key

GRACE_MINUTES = 2  # a minute counts as closed this many minutes after it started
WINDOW_MINUTES = 60  # the longest window we serve: older buckets are never read
INTERVAL_SECONDS = 60  # how often to trim

log = logging.getLogger("trimmer")


class BucketTrimmer:
    def __init__(self, redis_client, key_prefix, keep):
        self._redis = redis_client
        self._prefix = key_prefix
        self._keep = keep
        self._last_run = None

    def maybe_trim(self):
        """Call often (e.g. every loop pass). Does the work at most once per interval."""
        now = time.monotonic()
        if self._last_run is not None and now - self._last_run < INTERVAL_SECONDS:
            return
        self._last_run = now
        removed = self.trim_closed()
        if removed:
            log.info("trimmed %d members from closed %s buckets", removed, self._prefix)

    def trim_closed(self):
        """Trim every closed bucket that a window can still read. Returns members removed."""
        current_minute = int(time.time()) // 60
        oldest = current_minute - WINDOW_MINUTES
        newest_closed = current_minute - GRACE_MINUTES  # never touch the live buckets

        pipe = self._redis.pipeline(transaction=False)
        for minute in range(oldest, newest_closed + 1):
            # Ranks count up from the lowest score, so 0..-(keep+1) is everything except
            # the top `keep`. A small bucket is a no-op. Safe to repeat, and safe if
            # several consumer replicas run it.
            pipe.zremrangebyrank(bucket_key(self._prefix, minute), 0, -(self._keep + 1))
        return sum(pipe.execute())
