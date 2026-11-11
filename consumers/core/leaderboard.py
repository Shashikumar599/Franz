"""A consumer that counts events per member into per-minute Redis buckets.

Shared by every leaderboard group. A concrete group only says which topic and group id
to use, which Redis key prefix to write to, and how to turn one event into
(member, event_time_seconds).
"""
from abc import abstractmethod
from typing import Optional, Tuple

from core.buckets import BucketWriter
from core.stream_consumer import StreamConsumer
from core.trimmer import BucketTrimmer


class LeaderboardConsumer(StreamConsumer):
    @property
    @abstractmethod
    def key_prefix(self) -> str:
        """Redis key prefix of the minute buckets, e.g. "bsky:likes:"."""

    trim_keep: Optional[int] = None  # keep only this many members per closed bucket (None = all)

    def __init__(self, redis_client):
        self._writer = BucketWriter(redis_client, self.key_prefix)
        self._trimmer = (
            BucketTrimmer(redis_client, self.key_prefix, self.trim_keep) if self.trim_keep else None
        )

    @abstractmethod
    def to_count(self, event: dict) -> Optional[Tuple[str, int]]:
        """Return (member, event time in epoch seconds), or None if the event is unusable."""

    def handle_batch(self, events):
        items = []
        for event in events:
            item = self.to_count(event)
            if item is not None:
                items.append(item)
        return self._writer.add_batch(items)

    def maintenance(self):
        if self._trimmer is not None:
            self._trimmer.maybe_trim()
