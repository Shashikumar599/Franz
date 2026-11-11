"""Run one stream producer.

    python producers/main.py wikimedia
    python producers/main.py bluesky
"""
import argparse
import logging
import os

from core.runner import run
from sources import SOURCES

# In Docker this is a mounted volume, so resume points survive container recreation.
DEFAULT_CHECKPOINT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "checkpoints")


def main():
    parser = argparse.ArgumentParser(description="Stream -> Kafka producer")
    parser.add_argument("source", choices=sorted(SOURCES))
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    run(SOURCES[args.source](), os.environ.get("CHECKPOINT_DIR", DEFAULT_CHECKPOINT_DIR))


if __name__ == "__main__":
    main()
