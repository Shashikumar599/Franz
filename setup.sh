#!/usr/bin/env bash
# Bring the whole system up, stop it, or show its state.
#
#   ./setup.sh up       start everything (first run builds images and creates topics)
#   ./setup.sh down     stop everything; Kafka and Redis data are kept
#   ./setup.sh status   list the containers
#
# Needs Docker with Compose v2. Safe to run `up` again: it only starts what is missing.
set -euo pipefail
cd "$(dirname "$0")"

compose() { docker compose "$@"; }

topics() {
  local kt="docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092"
  echo "waiting for Kafka..."
  for _ in $(seq 1 60); do
    if $kt --list >/dev/null 2>&1; then break; fi
    sleep 2
  done
  $kt --list >/dev/null || { echo "Kafka did not come up"; exit 1; }
  # 24 h retention: the default 7 days of three replicas fills Docker's disk.
  $kt --create --if-not-exists --topic bsky.likes --partitions 12 --replication-factor 3 --config retention.ms=86400000
  $kt --create --if-not-exists --topic wiki.edits --partitions 6  --replication-factor 3 --config retention.ms=86400000
}

up() {
  compose -f docker-compose.yml up -d                       # Kafka x3 (KRaft) + Kafka UI
  topics
  compose -f redis/docker-compose.yml up -d                 # Redis + RedisInsight
  compose -f producers/deploy/wikimedia.yml up -d --build   # one instance per feed, never more
  compose -f producers/deploy/bluesky.yml up -d --build
  compose -f consumers/deploy/wiki-leaderboard.yml up -d --build --scale wiki-leaderboard=3
  compose -f consumers/deploy/bsky-likes-leaderboard.yml up -d --build --scale bsky-likes-leaderboard=3
  compose -f bluesky/api/docker-compose.yml up -d --build   # query service + UI
  cat <<EOF

Up. Give it a minute for the first counts to arrive.
  Leaderboard UI   http://localhost:8000
  API docs         http://localhost:8000/docs
  Kafka UI         http://localhost:8081
  RedisInsight     http://localhost:5540   (connect to redis://redis:6379)
EOF
}

down() {
  compose -f bluesky/api/docker-compose.yml down
  compose -f consumers/deploy/bsky-likes-leaderboard.yml down
  compose -f consumers/deploy/wiki-leaderboard.yml down
  compose -f producers/deploy/bluesky.yml down
  compose -f producers/deploy/wikimedia.yml down
  compose -f redis/docker-compose.yml down
  compose -f docker-compose.yml down
}

status() {
  docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' \
    | grep -E 'NAMES|kafka|redis|producer|consumer|api' || true
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  status) status ;;
  *) echo "usage: $0 up|down|status"; exit 1 ;;
esac
