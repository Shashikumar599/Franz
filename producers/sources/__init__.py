"""Available stream producers, by name. To add a source: write a StreamProducer and list it here."""
from sources.bluesky import BlueskyLikesProducer
from sources.wikimedia import WikimediaProducer

SOURCES = {cls.name: cls for cls in (WikimediaProducer, BlueskyLikesProducer)}
