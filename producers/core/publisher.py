"""Publish messages to Kafka and report acknowledgements to an AckTracker."""
import functools
import json
import logging
import os

from confluent_kafka import Producer

# Starting values, to be discussed one by one.
DEFAULT_CONFIG = {
    # One address is enough to discover the cluster; listing all keeps us connecting
    # if one broker is down. Docker sets the INTERNAL addresses, the host uses localhost.
    "bootstrap.servers": os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
    ),
    "acks": "all",  # the write counts only once all in-sync replicas have it
    "enable.idempotence": True,  # retries cannot create duplicates or reorder
    "linger.ms": 20,  # wait up to 20 ms to batch messages together
}

log = logging.getLogger("publisher")


class KafkaPublisher:
    def __init__(self, topic, tracker, client_id):
        self._topic = topic
        self._tracker = tracker
        self._producer = Producer({**DEFAULT_CONFIG, "client.id": client_id})

    def send(self, event_id, key, value):
        """Queue one message for Kafka. Returns immediately (asynchronous).

        key decides the partition: messages with the same key stay in order.
        """
        ticket = self._tracker.register(event_id)
        callback = functools.partial(self._on_delivery, ticket)
        payload = json.dumps(value, ensure_ascii=False)

        while True:
            try:
                self._producer.produce(self._topic, key=key, value=payload, on_delivery=callback)
                break
            except BufferError:
                # Local queue is full (Kafka slower than the stream): wait for acks.
                self._producer.poll(1)

        # produce() only queues. poll() runs the delivery callbacks of finished sends.
        self._producer.poll(0)

    def _on_delivery(self, ticket, err, msg):
        if err is not None:
            # The watermark stops at this event, so a restart replays it.
            log.error("delivery failed for %s: %s", msg.key(), err)
            return
        self._tracker.ack(ticket)

    def flush(self, timeout=10):
        """Block until queued messages are delivered (runs their callbacks)."""
        remaining = self._producer.flush(timeout)
        if remaining:
            log.warning("%d messages were not delivered before shutdown", remaining)
