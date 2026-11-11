"""Tracks which source events Kafka has acknowledged, to get a safe resume point.

Kafka acks can arrive out of order (different partitions, retries), so "the last
acked id" is not safe: event 7 may be acked while event 5 is still in flight.

Instead we keep a watermark: the newest event id for which it AND every earlier
event is acked. Resuming from it never skips an event (it may replay some).
"""
from collections import deque


class _Entry:
    def __init__(self, event_id):
        self.event_id = event_id
        self.acked = False


class AckTracker:
    def __init__(self, start_id=None):
        self.watermark = start_id  # safe resume point
        self._in_flight = deque()  # entries in send order, oldest first

    def register(self, event_id):
        """Call when an event is handed to Kafka. Returns a ticket for ack()."""
        entry = _Entry(event_id)
        self._in_flight.append(entry)
        return entry

    def skip(self, event_id):
        """An event we chose not to publish: handled, so the watermark may pass it."""
        self.ack(self.register(event_id))

    def ack(self, entry):
        """Call when Kafka confirms delivery of that event."""
        entry.acked = True
        # Move the watermark past every acked entry at the front of the queue.
        while self._in_flight and self._in_flight[0].acked:
            self.watermark = self._in_flight.popleft().event_id
