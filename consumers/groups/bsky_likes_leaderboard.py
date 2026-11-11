"""Group bsky-likes-leaderboard: likes per Bluesky post, per minute."""
from typing import Optional, Tuple

from core.leaderboard import LeaderboardConsumer


class BlueskyLikesLeaderboard(LeaderboardConsumer):
    group_id = "bsky-likes-leaderboard"
    topic = "bsky.likes"
    key_prefix = "bsky:likes:"
    batch_size = 500  # likes are small and frequent
    trim_keep = 100  # keep the top 100 posts of each closed minute

    def to_count(self, event) -> Optional[Tuple[str, int]]:
        post, timestamp = event.get("post"), event.get("ts")
        if not post or timestamp is None:
            return None
        try:
            return post, int(timestamp)  # event time, epoch seconds
        except (TypeError, ValueError):
            return None
