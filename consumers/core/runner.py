"""Kafka mechanics shared by every consumer group: poll in batches, hand events over, commit."""
import json
import logging
import os
import signal

from confluent_kafka import Consumer, KafkaException

POLL_TIMEOUT = 1.0  # seconds to wait for messages before returning an empty batch

log = logging.getLogger("runner")


def kafka_config(group_id):
    return {
        # Docker sets the INTERNAL broker addresses; on the host they are localhost ports.
        "bootstrap.servers": os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094"
        ),
        "group.id": group_id,  # consumers sharing this id split the partitions
        "auto.offset.reset": "earliest",  # no saved offset yet: start at the oldest message
        "enable.auto.commit": False,  # we commit ourselves, after the work is done
    }


def decode_events(batch):
    """Turn Kafka messages into event dicts. Unusable messages are skipped, not fatal."""
    events = []
    for msg in batch:
        if msg.error():
            if msg.error().fatal():
                raise KafkaException(msg.error())
            log.error("consumer error: %s", msg.error())
            continue
        try:
            event = json.loads(msg.value())
            if not isinstance(event, dict):
                raise ValueError("event is not a JSON object")
        except (ValueError, TypeError):
            # Skip it so a poison message cannot block the partition (DLQ comes later).
            log.warning("skipping bad message at p%d o%d", msg.partition(), msg.offset())
            continue
        events.append(event)
    return events


def _log_assigned(consumer, partitions):
    log.info("assigned partitions: %s", sorted(p.partition for p in partitions))


def _log_revoked(consumer, partitions):
    log.info("revoked partitions: %s", sorted(p.partition for p in partitions))


def _request_stop(signum, frame):
    # `docker stop` sends SIGTERM. Handle it like Ctrl+C so we leave the group cleanly.
    raise KeyboardInterrupt


def run(group):
    signal.signal(signal.SIGTERM, _request_stop)
    consumer = Consumer(kafka_config(group.group_id))
    consumer.subscribe([group.topic], on_assign=_log_assigned, on_revoke=_log_revoked)
    log.info("[%s] consuming %s", group.group_id, group.topic)

    try:
        while True:
            batch = consumer.consume(num_messages=group.batch_size, timeout=POLL_TIMEOUT)
            group.maintenance()
            if not batch:
                continue

            counted = group.handle_batch(decode_events(batch))

            # Commit only after the work succeeded. If handle_batch raised, we exit without
            # committing, so the batch is delivered again on restart (at-least-once).
            consumer.commit(asynchronous=False)
            log.info("[%s] batch of %d messages: %d counted", group.group_id, len(batch), counted)
    except KeyboardInterrupt:
        log.info("[%s] stopping", group.group_id)
    finally:
        consumer.close()  # leave the group cleanly so partitions are reassigned at once
