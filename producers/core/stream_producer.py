"""The producer interface: a producer only knows how to read ONE stream.

Subclasses implement two small methods:
  connect()     open one connection to the source and yield (cursor, raw_event)
  to_message()  turn a raw event into a Kafka Message, or None to skip it

Everything else (reconnect with backoff, publishing, acks, checkpoints) is shared,
so adding a source means writing one small class.
"""
import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Iterator, NamedTuple, Optional, Tuple

MAX_BACKOFF = 60  # cap for the reconnect delay, seconds

log = logging.getLogger("stream_producer")


class Message(NamedTuple):
    key: str  # decides the Kafka partition: same key -> same partition -> ordered
    value: dict  # JSON-serialisable payload


class StreamProducer(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """Short id used for logs, the checkpoint file and the Kafka client.id."""

    @property
    @abstractmethod
    def topic(self) -> str:
        """Kafka topic this producer publishes to."""

    # Errors that mean "the connection broke, reconnect" (anything else is a bug and propagates).
    retryable_errors: Tuple[type, ...] = (OSError,)

    @abstractmethod
    def connect(self, cursor: Optional[Any]) -> Iterator[Tuple[Any, Any]]:
        """Open ONE connection, resuming after `cursor` if given.

        Yield (cursor, raw_event) until the connection ends or breaks. The cursor is
        whatever the source uses as a resume point (event id, sequence number, ...).
        """

    @abstractmethod
    def to_message(self, raw_event: Any) -> Optional[Message]:
        """Return the Message to publish, or None to skip this event."""

    def stream(self, start_cursor: Optional[Any] = None) -> Iterator[Tuple[Any, Any]]:
        """Endless (cursor, raw_event) generator. Reconnects with exponential backoff."""
        last_cursor, delay = start_cursor, 1
        while True:
            try:
                for cursor, raw_event in self.connect(last_cursor):
                    last_cursor, delay = cursor, 1  # healthy again: reset backoff
                    yield cursor, raw_event
                # Normal end, e.g. the server closes long connections.
                log.info("[%s] stream closed by server, reconnecting", self.name)
                time.sleep(1)
            except self.retryable_errors as exc:
                log.warning("[%s] stream error: %r, retrying in %ss", self.name, exc, delay)
                time.sleep(delay)
                delay = min(delay * 2, MAX_BACKOFF)
