"""Run one consumer group.

    python consumers/main.py wiki-leaderboard
    python consumers/main.py bsky-likes-leaderboard
"""
import argparse
import logging
import os

import redis

from core.runner import run
from groups import GROUPS


def main():
    parser = argparse.ArgumentParser(description="Kafka consumer group -> Redis")
    parser.add_argument("group", choices=sorted(GROUPS))
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    redis_client = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))
    run(GROUPS[args.group](redis_client))


if __name__ == "__main__":
    main()
