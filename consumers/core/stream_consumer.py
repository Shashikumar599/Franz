"""The consumer interface: a consumer group only knows what to do with a batch of events.

The Kafka mechanics (polling, decoding, committing offsets, rebalance logging, shutdown)
live in core/runner.py and are the same for every group.
"""
from abc import ABC, abstractmethod
from typing import List


class StreamConsumer(ABC):
    @property
    @abstractmethod
    def group_id(self) -> str:
        """Kafka consumer group id. Consumers sharing it split the topic's partitions."""

    @property
    @abstractmethod
    def topic(self) -> str:
        """Kafka topic to read."""

    batch_size: int = 100  # max messages handled (and committed) at a time

    @abstractmethod
    def handle_batch(self, events: List[dict]) -> int:
        """Process decoded events and return how many were counted.

        When this returns, the work must be done: the runner commits the Kafka offsets
        right after. If it raises, nothing is committed and the batch is delivered again
        (at-least-once), so duplicates are possible.
        """

    def maintenance(self) -> None:
        """Called on every loop pass (about once a second), even with no messages."""
