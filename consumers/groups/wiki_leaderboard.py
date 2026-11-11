"""Group wiki-leaderboard: edits per Wikipedia page, per minute."""
from typing import Optional, Tuple

from core.leaderboard import LeaderboardConsumer


class WikiLeaderboard(LeaderboardConsumer):
    group_id = "wiki-leaderboard"
    topic = "wiki.edits"
    key_prefix = "wiki:title:"

    def to_count(self, event) -> Optional[Tuple[str, int]]:
        title, timestamp = event.get("title"), event.get("timestamp")
        if not title or timestamp is None:
            return None
        try:
            return title, int(timestamp)  # event time, epoch seconds
        except (TypeError, ValueError):
            return None
