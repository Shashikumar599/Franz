from datetime import datetime, timezone

from sources.bluesky import BlueskyLikesProducer
from sources.wikimedia import WikimediaProducer, parse_sse


# ---- Wikimedia ----------------------------------------------------------------

def test_parse_sse_groups_lines_into_events():
    lines = ["id: 1", 'data: {"a":1}', "", ": keep-alive", "event: error", "data: boom", ""]
    assert list(parse_sse(lines)) == [
        ("message", "1", '{"a":1}'),
        ("error", "1", "boom"),
    ]


def test_wikimedia_keeps_enwiki_edits_keyed_by_title():
    event = {"wiki": "enwiki", "type": "edit", "title": "Kafka"}
    message = WikimediaProducer().to_message(event)
    assert message.key == "Kafka"
    assert message.value == event


def test_wikimedia_keeps_new_pages():
    assert WikimediaProducer().to_message({"wiki": "enwiki", "type": "new", "title": "X"})


def test_wikimedia_skips_other_wikis_types_and_missing_titles():
    producer = WikimediaProducer()
    assert producer.to_message({"wiki": "dewiki", "type": "edit", "title": "X"}) is None
    assert producer.to_message({"wiki": "enwiki", "type": "categorize", "title": "X"}) is None
    assert producer.to_message({"wiki": "enwiki", "type": "edit"}) is None


# ---- Bluesky ------------------------------------------------------------------

POST = "at://did:plc:abc/app.bsky.feed.post/3k"


def like(post=POST, operation="create", collection="app.bsky.feed.like",
         time="2026-08-13T06:47:43.959305Z"):
    return {
        "collection": collection,
        "operation": operation,
        "time": time,
        "did": "did:plc:liker",
        "seq": 7,
        "record": {"subject": {"uri": post}},
    }


def test_bluesky_like_becomes_a_message_keyed_by_post():
    message = BlueskyLikesProducer().to_message(like())
    expected_ts = int(datetime(2026, 8, 13, 6, 47, 43, tzinfo=timezone.utc).timestamp())
    assert message.key == POST
    assert message.value == {"post": POST, "liker": "did:plc:liker", "ts": expected_ts}


def test_bluesky_skips_unlikes_other_collections_and_non_post_subjects():
    producer = BlueskyLikesProducer()
    assert producer.to_message(like(operation="delete")) is None
    assert producer.to_message(like(collection="app.bsky.feed.repost")) is None
    assert producer.to_message(like(post="at://did:plc:abc/app.bsky.feed.generator/g")) is None


def test_bluesky_skips_events_without_a_usable_time():
    assert BlueskyLikesProducer().to_message(like(time=None)) is None
    assert BlueskyLikesProducer().to_message(like(time="not a time")) is None
