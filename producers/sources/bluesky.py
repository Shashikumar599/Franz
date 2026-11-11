"""Bluesky Jetstream likes (WebSocket) -> topic bsky.likes, keyed by the liked post."""
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import websocket  # package: websocket-client

from core.stream_producer import Message, StreamProducer

URL = "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents"
# Server-side filter: only like records, only commit events (no identity/account noise).
PARAMS = "collections=app.bsky.feed.like&kinds=commit"
READ_TIMEOUT = 60  # seconds of silence before we treat the connection as dead

LIKE_COLLECTION = "app.bsky.feed.like"
POST_MARKER = "/app.bsky.feed.post/"  # likes can also target feed generators, lists, ...

log = logging.getLogger("bluesky")


def _epoch_seconds(iso_time):
    """'2026-08-13T06:47:43.959305Z' -> epoch seconds (whole seconds, UTC)."""
    try:
        parsed = datetime.strptime(iso_time[:19], "%Y-%m-%dT%H:%M:%S")
    except (TypeError, ValueError):
        return None
    return int(parsed.replace(tzinfo=timezone.utc).timestamp())


class BlueskyLikesProducer(StreamProducer):
    name = "bluesky"
    topic = "bsky.likes"
    retryable_errors = (websocket.WebSocketException, OSError)

    def connect(self, cursor):
        url = f"{URL}?{PARAMS}"
        if cursor is not None:
            url += f"&cursor={cursor}"  # resume point (inclusive: that event comes again)

        ws = websocket.create_connection(url, timeout=READ_TIMEOUT)
        try:
            while True:
                raw = ws.recv()  # blocks until a message arrives; pings are answered for us
                try:
                    message = json.loads(raw)
                except (TypeError, ValueError):
                    log.warning("skipping non-JSON message: %.200r", raw)
                    continue
                # The docs wrap the event as {"$type": "message", "payload": {...}}.
                payload = message.get("payload", message)
                seq = payload.get("seq")
                if seq is None:
                    continue  # no sequence number: nothing to resume from
                yield seq, payload
        finally:
            ws.close()

    def to_message(self, raw_event) -> Optional[Message]:
        """A new like on a post -> Message keyed by the post's AT URI, else None.

        Unlikes (operation "delete") carry no post URI, so they cannot be counted down.
        """
        if raw_event.get("collection") != LIKE_COLLECTION or raw_event.get("operation") != "create":
            return None
        subject = (raw_event.get("record") or {}).get("subject") or {}
        post = subject.get("uri")
        if not post or POST_MARKER not in post:
            return None
        ts = _epoch_seconds(raw_event.get("time"))
        if ts is None:
            return None
        return Message(key=post, value={"post": post, "liker": raw_event.get("did"), "ts": ts})
