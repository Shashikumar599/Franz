"""Keeps the cache warm: recomputes every recently requested window in the background.

A request registers its window with `touch()`. For the next `active_for_s` seconds a
background loop keeps recomputing that window, so later requests find a fresh entry
already in the cache and never compute anything themselves.
"""
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait

from leaderboard import window_name

log = logging.getLogger("refresher")


class Refresher:
    def __init__(self, cache, compute, interval_s, active_for_s=60.0, max_workers=8):
        self._cache = cache
        self._compute = compute  # compute(minutes) -> result dict
        self._interval_s = interval_s  # pause between refresh cycles
        self._active_for_s = active_for_s  # stop refreshing a window nobody asks for
        self._last_requested = {}  # minutes -> time.monotonic() of the last request
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="refresh")
        self._thread = threading.Thread(target=self._run, name="refresher", daemon=True)

    def touch(self, minutes):
        """A request asked for this window: keep it fresh for a while."""
        with self._lock:
            self._last_requested[minutes] = time.monotonic()

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)
        self._pool.shutdown(wait=False)

    def _active_windows(self):
        cutoff = time.monotonic() - self._active_for_s
        with self._lock:
            self._last_requested = {m: t for m, t in self._last_requested.items() if t >= cutoff}
            return list(self._last_requested)

    def _run(self):
        while not self._stop.is_set():
            # One cycle: refresh all active windows in parallel, wait for the slowest.
            futures = [self._pool.submit(self._refresh_one, m) for m in self._active_windows()]
            wait(futures)
            self._stop.wait(self._interval_s)

    def _refresh_one(self, minutes):
        try:
            # Skipped if another process (another API instance) already holds the lock.
            self._cache.refresh(window_name(minutes), lambda: self._compute(minutes))
        except Exception:
            log.exception("refresh failed for the %d-minute window", minutes)
