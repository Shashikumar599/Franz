# Franz

Live leaderboards built on Kafka. The data is real: every Bluesky like and every English Wikipedia
edit comes in from public streams, goes through a 3-broker Kafka cluster, and is counted per minute
in Redis. An API and a small UI serve the most liked Bluesky posts over the last 1 to 60 minutes.
Wikipedia edits are counted the same way but are not served yet.

I built it to learn Kafka properly: partitions, keys, consumer groups, offsets, replication, and
what breaks under load. Parts of the code and UI still say WikiPulse, the project's earlier name.

```
Bluesky Jetstream ─► producer ─► bsky.likes (12 partitions) ─► consumer group ×3 ─┐
Wikimedia stream  ─► producer ─► wiki.edits  (6 partitions) ─► consumer group ×3 ─┤
                                 Kafka 4.0, 3 KRaft nodes                           ▼
                                 replication 3, min.insync 2                Redis, one sorted set per minute
                                                                                    │
                                                                       query service + UI (:8000)
```
https://github.com/user-attachments/assets/92f6db31-0c95-46e0-97c3-cd5470a9b6d0

## Run it

You need Docker with Compose v2. Give Docker at least 6 GB of memory: three Kafka brokers take
about 1 GB each. The machine needs internet access for the live streams.

```
./setup.sh up        # build, start everything, create the topics
./setup.sh status
./setup.sh down      # stop; Kafka and Redis data are kept
```

The first `up` builds three images and takes a few minutes. Counts start appearing within a minute.

| | |
|---|---|
| Leaderboard | http://localhost:8000 |
| API | http://localhost:8000/top-liked?minutes=15&limit=10 |
| Kafka UI | http://localhost:8081 |
| RedisInsight | http://localhost:5540 (connect to `redis://redis:6379`) |

To look at Kafka from the command line, run the tools inside a broker and use the internal
listener (`kafka-1:19092`). `localhost` inside the container points at the wrong port:

```
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic bsky.likes
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:19092 --describe --group bsky-likes-leaderboard
```

## How it works

- **Producers** keep one connection to each upstream stream and publish every event with a key:
  the post URI for likes, the page title for edits. The same key always goes to the same partition.
  Writes use `acks=all` with idempotence on. Each producer saves the position of the last event
  Kafka confirmed and resumes from it after a restart. Run exactly one per stream; a second would
  publish everything twice.
- **Kafka** runs three nodes in KRaft mode (each is broker and controller). Topics have three
  replicas and need two in sync to accept a write, so one broker can be down without losing writes.
- **Consumers** read in batches of up to 500, write each batch to Redis in one transaction, and only
  then commit the offset. A crash before the commit means the batch is read again (at-least-once).
  Each group has three replicas sharing the partitions.
- **Redis** holds one sorted set per minute: member = post or page, score = count. A like is counted
  in the minute it happened, not when it arrived, so delays and replays don't create spikes. Minutes
  expire after 65 minutes. Closed Bluesky minutes are trimmed to their top 100 (a minute holds
  around 14,000 posts).
- **Query service** (FastAPI, Bluesky only) answers "top k in the last N minutes" by merging the last N minutes in
  Redis. A background refresher keeps recently requested windows precomputed in a cache, and a lock
  in Redis makes sure only one process recomputes a missing one.

## Results

Measured on a MacBook Air M1 (16 GB), with everything above running at once:

- **Writes:** 9,600 likes/s counted end to end during a replay, with Redis at 4 % CPU.
- **Reads:** 1,200 random queries/s from one API process with no errors, p99 36 ms server-side.
- Two changes made most of the difference: reading only the top 1,000 of the busiest minutes
  (merge time 20 ms → 1.5 ms), and running uvicorn with its C extensions plus direct JSON encoding
  (ceiling 1,000 → 1,200 req/s).

The full write-up, with every test run, is in [findings/](findings/).

## Repository

| | |
|---|---|
| `docker-compose.yml` | Kafka cluster and Kafka UI |
| `redis/` | Redis (append-only file on) and RedisInsight |
| `producers/` | one image, one entry point per stream (`python main.py bluesky\|wikimedia`), compose files in `deploy/` |
| `consumers/` | one image, one entry point per consumer group, compose files in `deploy/` |
| `bluesky/api/` | query service, cache, refresher, UI |
| `loadtest/` | load and stress test scripts (Python and vegeta), system monitor |
| `findings/` | results and discussion |
| `setup.sh` | start, stop, status |

## Tests

```
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest producers
python -m pytest consumers      # run separately: both packages are called core
```

## Load testing

`loadtest/load_test.py` sends random queries on a fixed schedule (uniform, Poisson or bursts) and
measures latency from the scheduled send time. `loadtest/step_test.py` raises a steady rate with
[vegeta](https://github.com/tsenart/vegeta) until something breaks. `loadtest/monitor.py` records
Docker, Redis, Kafka lag and host usage alongside. Output goes to `loadtest/results/`, which is not
committed.

## Known limits

- One producer per stream is a single point of failure. It recovers from a crash, but nothing flows
  while it is down, and a connection that falls behind is not detected.
- No deduplication yet: events replayed after a crash are counted twice.
- Wikipedia counts include bot edits and have no API yet.
- Unlikes are not subtracted, and long windows are close but not exact because of the top-100 trim.
- Each API worker process runs its own refresher, so adding workers multiplies the background load
  on Redis.
