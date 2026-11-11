"""Wikimedia recentchange (Server-Sent Events) -> topic wiki.edits, keyed by page title."""
import json
import logging
from typing import Optional

import requests

from core.stream_producer import Message, StreamProducer

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {
    "Accept": "text/event-stream",
    "User-Agent": "WikiPulse-learning/0.1 (Kafka learning project)",
}
CONNECT_TIMEOUT = 10  # seconds to establish the connection
READ_TIMEOUT = 60  # seconds of silence before we treat the connection as dead

WIKI = "enwiki"  # English Wikipedia only: the same page has different titles per language
COUNTED_TYPES = {"edit", "new"}  # skip categorize/log events: they are not page edits

log = logging.getLogger("wikimedia")


def parse_sse(lines):
    """Group raw SSE lines into events: yields (event_type, event_id, data).

    Wire format: `field: value` lines, one event ends at a blank line.
    Lines starting with ':' are keep-alive comments.
    """
    event_type, event_id, data = "message", None, []
    for line in lines:
        if line == "":  # blank line = event complete
            if data:
                yield event_type, event_id, "\n".join(data)
            event_type, data = "message", []
        elif line.startswith(":"):
            continue
        else:
            field, _, value = line.partition(":")
            value = value.lstrip(" ")
            if field == "event":
                event_type = value
            elif field == "id":
                event_id = value
            elif field == "data":
                data.append(value)


class WikimediaProducer(StreamProducer):
    name = "wikimedia"
    topic = "wiki.edits"
    retryable_errors = (requests.RequestException,)

    def connect(self, cursor):
        headers = dict(HEADERS)
        if cursor:
            headers["Last-Event-ID"] = cursor  # ask the server to resume

        # stream=True: don't wait for the (endless) body, read it as it arrives.
        with requests.get(
            URL, headers=headers, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        ) as resp:
            resp.raise_for_status()
            resp.encoding = "utf-8"  # SSE has no charset; requests would guess latin-1

            for event_type, event_id, data in parse_sse(resp.iter_lines(decode_unicode=True)):
                if event_type == "error":
                    log.error("server sent an error event: %s", data)
                    continue
                if event_id is None:
                    continue  # nothing to resume from
                try:
                    yield event_id, json.loads(data)
                except json.JSONDecodeError:
                    # One bad event must not kill the stream.
                    log.warning("skipping malformed event: %.200s", data)

    def to_message(self, raw_event) -> Optional[Message]:
        if raw_event.get("wiki") != WIKI or raw_event.get("type") not in COUNTED_TYPES:
            return None
        title = raw_event.get("title")
        if not title:
            return None
        return Message(key=title, value=raw_event)
