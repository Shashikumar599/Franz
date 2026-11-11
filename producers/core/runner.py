"""Shared run loop: read a stream, publish to Kafka, remember how far we got."""
import logging
import os
import signal
import time

from core.ack_tracker import AckTracker
from core.checkpoint import Checkpoint
from core.publisher import KafkaPublisher

CHECKPOINT_INTERVAL = 5  # seconds between checkpoint writes

log = logging.getLogger("runner")


def _request_stop(signum, frame):
    # `docker stop` sends SIGTERM. Handle it like Ctrl+C so we flush and save first.
    raise KeyboardInterrupt


def run(producer, checkpoint_dir):
    signal.signal(signal.SIGTERM, _request_stop)
    os.makedirs(checkpoint_dir, exist_ok=True)

    checkpoint = Checkpoint(os.path.join(checkpoint_dir, f"{producer.name}.json"))
    start_cursor = checkpoint.load()
    log.info("[%s] starting from %s", producer.name, start_cursor or "now (no checkpoint)")

    tracker = AckTracker(start_cursor)
    publisher = KafkaPublisher(producer.topic, tracker, client_id=f"{producer.name}-producer")
    last_save = time.monotonic()
    sent = skipped = 0

    try:
        for cursor, raw_event in producer.stream(start_cursor):
            message = producer.to_message(raw_event)
            if message is None:
                tracker.skip(cursor)  # never published, but counts as handled
                skipped += 1
            else:
                publisher.send(cursor, message.key, message.value)
                sent += 1

            if time.monotonic() - last_save >= CHECKPOINT_INTERVAL:
                checkpoint.save(tracker.watermark)
                last_save = time.monotonic()
                log.info("[%s] published=%d skipped=%d", producer.name, sent, skipped)
    except KeyboardInterrupt:
        log.info("[%s] stopping", producer.name)
    finally:
        publisher.flush()  # wait for outstanding acks first...
        checkpoint.save(tracker.watermark)  # ...then save the final position
