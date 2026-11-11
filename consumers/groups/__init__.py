"""Available consumer groups, by group id. To add one: write a StreamConsumer and list it here."""
from groups.bsky_likes_leaderboard import BlueskyLikesLeaderboard
from groups.wiki_leaderboard import WikiLeaderboard

GROUPS = {cls.group_id: cls for cls in (WikiLeaderboard, BlueskyLikesLeaderboard)}
