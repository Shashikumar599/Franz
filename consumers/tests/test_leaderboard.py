import time

from core.buckets import BUCKET_TTL_SECONDS, BucketWriter
from core.trimmer import BucketTrimmer
from groups.bsky_likes_leaderboard import BlueskyLikesLeaderboard
from groups.wiki_leaderboard import WikiLeaderboard


class FakePipeline:
    def __init__(self, transaction):
        self.transaction = transaction
        self.commands = []

    def zincrby(self, key, amount, member):
        self.commands.append(("zincrby", key, amount, member))

    def expireat(self, key, when):
        self.commands.append(("expireat", key, when))

    def zremrangebyrank(self, key, start, stop):
        self.commands.append(("zremrangebyrank", key, start, stop))

    def execute(self):
        return [0] * len(self.commands)


class FakeRedis:
    def __init__(self):
        self.pipelines = []

    def pipeline(self, transaction=True):
        pipe = FakePipeline(transaction)
        self.pipelines.append(pipe)
        return pipe


# ---- turning events into (member, time) ----------------------------------------

def test_wiki_group_counts_titles_by_event_time():
    group = WikiLeaderboard(FakeRedis())
    assert group.to_count({"title": "Kafka", "timestamp": 1700000000}) == ("Kafka", 1700000000)


def test_wiki_group_skips_unusable_events():
    group = WikiLeaderboard(FakeRedis())
    assert group.to_count({"timestamp": 1}) is None
    assert group.to_count({"title": "Kafka"}) is None
    assert group.to_count({"title": "Kafka", "timestamp": "soon"}) is None


def test_bluesky_group_counts_posts_by_event_time():
    group = BlueskyLikesLeaderboard(FakeRedis())
    assert group.to_count({"post": "at://p", "ts": 1700000000}) == ("at://p", 1700000000)
    assert group.to_count({"ts": 1}) is None


def test_groups_have_distinct_ids_and_topics():
    redis = FakeRedis()
    wiki, bsky = WikiLeaderboard(redis), BlueskyLikesLeaderboard(redis)
    assert wiki.group_id != bsky.group_id
    assert wiki.topic != bsky.topic
    assert wiki.key_prefix != bsky.key_prefix


# ---- writing buckets ------------------------------------------------------------

def test_batch_is_one_transaction_with_expiry_and_aggregated_counts():
    redis = FakeRedis()
    now = int(time.time())
    minute = now // 60

    counted = BucketWriter(redis, "x:").add_batch([("a", now), ("a", now), ("b", now)])

    assert counted == 3
    assert len(redis.pipelines) == 1 and redis.pipelines[0].transaction is True
    commands = redis.pipelines[0].commands
    assert ("zincrby", f"x:{minute}", 2, "a") in commands
    assert ("zincrby", f"x:{minute}", 1, "b") in commands
    assert ("expireat", f"x:{minute}", minute * 60 + BUCKET_TTL_SECONDS) in commands


def test_events_older_than_an_hour_are_skipped():
    redis = FakeRedis()
    old = int(time.time()) - 2 * 3600
    assert BucketWriter(redis, "x:").add_batch([("a", old)]) == 0
    assert redis.pipelines == []  # nothing sent to Redis at all


# ---- trimming ---------------------------------------------------------------------

def test_trim_keeps_top_n_of_closed_buckets_only():
    redis = FakeRedis()
    BucketTrimmer(redis, "x:", keep=100).trim_closed()

    pipe = redis.pipelines[0]
    assert pipe.transaction is False
    assert len(pipe.commands) == 59  # minutes now-60 .. now-2: the live buckets are untouched
    assert all(c[0] == "zremrangebyrank" and c[2:] == (0, -101) for c in pipe.commands)


def test_trim_runs_at_most_once_per_interval():
    redis = FakeRedis()
    trimmer = BucketTrimmer(redis, "x:", keep=100)
    trimmer.maybe_trim()
    trimmer.maybe_trim()
    assert len(redis.pipelines) == 1
