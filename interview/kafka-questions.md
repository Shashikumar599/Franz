# Kafka interview practice log

Mock interview questions, one at a time, each focused on a single thing. For each: the question, a short summary of Shashi's answer, feedback, and the points a strong answer covers.

---

## Q1. Project architecture

**Question:** Walk me through the architecture of your project.

**Your answer (summary):** Goal is top-K posts by likes; same pattern as YouTube views or ad aggregation. Bluesky like events pushed to a "durable queue" (Kafka), keyed by post ID/link "so they go to different partitions"; ordering not very important. A dynamically scalable consumer group (3 consumers, 12 partitions) writes to Redis. A query service reads top-K for a window; results are cached and pre-computed by a background worker to keep p99 low.

**Feedback (7/10):** Much better structure, and framing it as a general stream-aggregation pattern is a strong opener. Gaps:
- "Durable queue" again: say **replicated, partitioned log**.
- Key explanation imprecise: the key guarantees **the same post always goes to the same partition** (hash of key mod partition count). Spreading load is a side effect, not the purpose.
- "Ordering not important" weakens the reason for keying. Expect a follow-up on it.
- The Redis design is the heart of the system and was skipped: **per-minute sorted sets**, event-time buckets, expiry after about 65 min, ZUNIONSTORE over the last N buckets.
- "Dynamically scalable" needs the limit: **up to the partition count** (12).
- Missing: single producer instance (one upstream feed), 3 brokers, RF 3, acks=all.
- Say numbers when you claim latency: 3–15 ms round trip after warm-up.

**Strong answer covers:**
1. Problem and pattern: top-K by likes over sliding windows of 1–60 min; same shape as view counts or ad clicks.
2. Ingest: one producer (the upstream feed is single, so more instances would duplicate data) reads Bluesky Jetstream over WebSocket and publishes likes to `bsky.likes`, keyed by liked-post URI, acks=all, idempotent.
3. Kafka: 3 brokers, 12 partitions, RF 3, min.insync.replicas 2, 24 h retention.
4. Processing: consumer group, scales up to 12 replicas; increments per-minute Redis sorted sets (ZINCRBY), commits offsets after the Redis write (at-least-once); closed buckets trimmed to top 100.
5. Serving: API sums the last N buckets with ZUNIONSTORE, single-flight cache, background refresher keeps active windows warm; 3–15 ms.
6. One line on why the log matters: buffering, replay (Redis is rebuildable), independent consumer groups.

---

## Q2. Why key by post if ordering doesn't matter

**Question:** You said ordering isn't important here. Then why use the post ID as the message key at all? What would change if you produced with no key?

**Your answer (summary):** We could drop the post ID key; that avoids the hot-key problem when one post gets unusually high traffic and spreads load better across partitions. The key was kept for the mental model and doesn't hurt, but no key is better; learning shouldn't be the design criterion.

**Feedback (5/10):** Hot-key awareness is good, but you gave up the design too fast. Keying has a real benefit here, and you didn't name it. Gaps:
- **Missed the main benefit of keying: locality.** All likes for a post reach one consumer, so it can **pre-aggregate** in memory (50 likes for post X in a batch → one `ZINCRBY +50`). Without a key, every consumer sees every hot post and the reduction shrinks. At 50k/s this decides how many Redis writes you need.
- "Doesn't hurt" is wrong at scale: a viral post means **one hot partition**. One consumer lags while the others sit idle, and adding consumers doesn't help.
- "Regular key" is vague: say **null key**. librdkafka's sticky partitioner then fills one partition's batch before moving on, which gives bigger batches and better compression.
- Never justify a design with "for learning" in an interview. Give the trade-off.
- Correctness is fine either way because ZINCRBY is commutative. Say so: it shows you checked whether ordering matters.

**Strong answer covers:**
1. What a key gives: same key → same partition → per-key ordering and **per-key locality** on one consumer.
2. Ordering isn't needed (counts are commutative), so the real reason to key is locality: local pre-aggregation that cuts Redis writes, and room for per-post state later (dedup, a Kafka Streams aggregate).
3. Cost: skew. A viral post means a hot partition, so one consumer lags and scaling out doesn't help.
4. No key: even spread (sticky partitioner, good batching), no hot partition; but no locality, so every consumer writes every hot post.
5. Middle ground: **key salting** (`post_id#0..N-1`) splits a hot key across N partitions while keeping some locality; the reader sums the parts. Or two-stage aggregation.
6. Data-driven call: about 8,600 distinct posts per minute, so skew is modest today; measure per-partition lag before changing.

**Correction (Shashi's pushback, valid):** each consumer can pre-aggregate its own batch without a key, so keying is not *required* for pre-aggregation. What keying changes is the **reduction factor**: writes ≈ distinct (post, consumer) pairs per batch. Keyed: a hot post gives 1 write per batch. Unkeyed: up to C writes (one partial per consumer). Long-tail posts with one like each: no difference. So with Redis as the central store, keying is only a constant-factor win (at most the consumer count) on hot posts. Keying becomes **decisive** when state lives *in the consumer* (Kafka Streams local state store, exact per-key windows without an external store, per-key ordering for like→unlike state): then all events for a key *must* meet in one place.

**Final answer (say this):**
> "Ordering doesn't matter here, because increments are commutative, so correctness is the same with or without a key. What the key changes is where a post's likes go: the same post always lands on the same partition, so one consumer sees all of them.
> With Redis as the central counter, that's only a modest win. Each consumer pre-aggregates its batch either way, but with a key a hot post becomes one write per batch instead of one partial write per consumer. The cost is skew: a viral post makes one partition hot, that consumer lags, and adding consumers can't help.
> With no key, the sticky partitioner spreads load evenly and batches well, and I lose very little. So for pure counting into Redis, no key, or salting only the hot keys, is the better default.
> I'd keep the key if the state moved into the consumer, say Kafka Streams with a local store, or per-user like/unlike tracking. Then every event for a key must reach the same instance, and keying is required, not optional.
> Today we see about 8,600 distinct posts a minute, so skew is modest; I'd check per-partition lag before changing it."

---

## Side question (asked by Shashi): DLQ when strict ordering is required

**Question:** Order management, strict ordering per order. Event 1 for an order fails and goes to the DLQ. How are later events for that order handled? We can't process them (order would break), and we can't block the partition (other orders are stuck behind it).

**Answer:**
1. **Split failures by type.** Transient (DB down, timeout): retry in place with bounded backoff; a short block is acceptable. Permanent (bad data, bug): park it; never retry forever.
2. **Block the key, not the partition ("park the key").**
   - Event for order X fails permanently → write it to a parked/DLQ topic **keyed by order ID**, and record X in a durable "blocked keys" store (DB/Redis, or a Kafka Streams state store).
   - Every later event: if its key is blocked, **divert it to the parked topic too** (in order) instead of processing it. Other orders keep flowing. Offsets keep committing, because the diverted events are safely stored.
   - After the fix: replay X's parked events in order. Keep diverting new X events until the parked backlog for X is empty, then unblock X (otherwise a new event can overtake a parked one).
3. **Make the consumer defend ordering itself.** Each event carries a per-order sequence/version. The consumer applies only `version == last + 1`, ignores duplicates (`<= last`) and parks gaps. That also covers producer bugs and replays, not just the DLQ.
4. **Don't use** plain DLQ-and-continue (breaks order for X) or non-blocking retry topics (`retry-5s`, `retry-1m`), unless combined with key parking; they reorder.
5. **Whole-partition stop** is a valid choice only where correctness beats availability (some ledgers stop the world deliberately): rare failures, paged on-call, fast fix.
6. **Ops:** alert when the DLQ is non-empty, export a "parked keys" gauge, provide a replay tool. Reduce poison messages upstream with schema validation (Schema Registry) at the producer.

---

## Q3. Hot partition

**Question:** Say one post goes viral and gets 40% of all likes. With your current keying, what exactly happens in your system, and how would you detect it?

**Your answer (summary):** It becomes a hot key; one partition is overloaded compared to the rest. Depends on how partitions were sized and their headroom; that partition may hit "the partition size limit". Fix: salt the key (`orderId_A`, `orderId_B`) so it spreads over several partitions.

**Feedback (5/10):** Hot key → hot partition is right, and salting is the right tool. Gaps:
- **"Partition capacity / size limit" isn't a Kafka thing.** There's no per-partition throughput cap to hit. The real limits are (a) the **one consumer** that owns the partition, (b) the **leader broker's** disk and network for that partition, and (c) `retention.bytes`, which deletes old data rather than rejecting writes.
- **Didn't walk through *your* system:** which consumer is hit, what lag does, why adding consumers doesn't help, what happens in Redis.
- **Detection was skipped**, and it was half the question.
- **Salting an order ID breaks per-order ordering.** That contradicts the side question; salting is fine only when order doesn't matter, as with like counts.
- Didn't mention the cost of salting: someone has to recombine the parts, and you need to know *which* keys are hot.

**Strong answer covers:**
1. **Producer and broker:** fine at this scale. One partition's leader takes 40% of the bytes plus replication to 2 followers, so broker load is uneven but far below limits.
2. **Consumer:** the instance owning that partition gets 40% of all traffic plus its other 3 partitions. If it can't keep up, **lag grows on that one partition** while the others sit idle. Adding consumers doesn't help: one partition maps to at most one consumer in a group.
3. **Redis:** a single member gets many ZINCRBYs. If the consumer aggregates per batch, the hot post costs about 1 write per batch, so skew mostly costs CPU for deserialisation, not Redis.
4. **Detection:** per-partition lag (`kafka-consumer-groups --describe`, Kafka UI, kafka-lag-exporter → Prometheus); per-partition produce rate (log-end-offset growth); alert on skew ratio = max partition rate / average; consumer-side top-key sampling per batch.
5. **Fixes:** salt hot keys `post#0..N-1`. Here the consumer strips the salt and writes the same Redis member, so recombining is free. Or drop the key entirely (counts don't need it). Never salt where per-key order matters (orders). Salting only hot keys needs a hot-key list (static, or fed back from consumer sampling).

**Final answer (say this):**
> "That post's likes all hash to one partition, so one consumer gets 40% of the traffic plus its other partitions. If it can't keep up, lag grows on that single partition while the other consumers sit idle, and adding consumers doesn't help, because a partition can only go to one consumer in a group. The broker leading that partition also takes more bytes and replication load, but that's far from a limit at our volume. Redis is fine: with per-batch aggregation the hot post is about one ZINCRBY per batch.
> I'd detect it from per-partition lag and per-partition produce rate, exported to Prometheus with an alert on max-to-average skew, plus sampling the top keys in the consumer.
> To fix it I'd salt the hot key, `post#0..N`. The consumer strips the salt and increments the same Redis member, so nothing downstream changes. For counts I could even drop the key. I would *not* salt something like order IDs, where per-key order matters."

---

## Q4. What limits a single partition (covered through discussion)

**Question:** You mentioned a partition hitting its capacity. What actually limits how much throughput a single partition can handle?

**Your reasoning:** A partition lives on one broker, and that broker hosts many partitions, so the broker's capacity is the partition's ceiling. Same for a consumer, which also handles several partitions. Assumed the partition (broker) is the bottleneck before the consumer. But consumers do more work per message, and both are machines with similar resources. Can a broker be a bottleneck?

**Answer:**
- **Right:** a partition is one log on one leader broker and never splits across brokers, so one partition's ceiling is a share of one broker (and of one consumer).
- **Usually inverted:** the **consumer hits its limit first**. Per message, the broker does very little: append opaque compressed batches sequentially to the page cache, let followers fetch them, and serve consumers with zero-copy `sendfile`. It never parses payloads. The consumer deserialises, runs logic and makes external I/O calls (Redis, DB). Rough orders: a partition leader handles tens of MB/s; a Python consumer doing a network call per batch handles far less.
- **When the broker does become the bottleneck:**
  - **Network egress:** produce in + (RF−1) replication out + one fetch stream **per consumer group**. Fan-out multiplies it.
  - **Page cache misses:** lagging or replaying consumers read old segments from disk and evict hot data. This hurts every partition on that broker.
  - **CPU:** TLS (disables zero-copy); compression mismatch between producer and topic (broker recompresses); format conversion for old clients.
  - **Request rate:** many tiny requests (`linger.ms=0`, small batches) saturate network/IO threads.
  - **acks=all latency:** bounded by the slowest in-sync follower.
  - **Noisy neighbour:** a hot partition makes its leader hot for *all* partitions it leads. Rebalance leadership or move partitions.
- Brokers and consumers are not similar machines: brokers are sized for disk, network and page cache (RAM); consumers for CPU and downstream I/O.
- **Sizing formula:** partitions ≥ max(target throughput / per-partition produce throughput, target throughput / per-consumer throughput), plus headroom. Measure both numbers.

**Final answer (say this):**
> "A partition is a single log led by one broker and consumed by one consumer in a group, so its ceiling is the smaller of the two. In practice the consumer usually limits first: the broker only appends opaque batches sequentially and serves them with zero-copy, while the consumer deserialises, runs logic and calls Redis or a DB. The broker becomes the limit through network egress (replication plus one stream per consumer group), page-cache misses from lagging consumers, TLS or recompression CPU, or too many tiny requests. I size partitions as target throughput divided by the smaller of measured per-partition produce and per-consumer throughput, plus headroom."

---

## Q5. Consumer restart and rebalance

**Question:** One of your 3 bsky consumers is restarted. Walk me through what happens in the group, from the moment it stops until it is consuming again.

**Your answer (summary):** 12 partitions spread over 3 consumers. When one restarts, some partitions are not consumed; Kafka redistributes them to the remaining two. When the consumer comes back and registers, partitions are redistributed across three again.

**Feedback (4/10):** The outcome is right (two rebalances: 3→2→3), but an interviewer asking "walk me through" wants the mechanics, and none were named. Gaps:
- **No group coordinator.** One broker manages the group (the leader of the group's `__consumer_offsets` partition; Kafka UI showed coordinator = 1).
- **No detection.** Graceful stop (SIGTERM → `close()`) sends **LeaveGroup**, so the rebalance is immediate. Crash or `kill -9`: only noticed after **`session.timeout.ms`** (45 s default) without heartbeats. Those partitions sit unconsumed meanwhile, so lag grows. A stuck poll loop is caught by **`max.poll.interval.ms`** (5 min default).
- **No rebalance protocol.** Eager (range/roundrobin): *every* consumer revokes *all* partitions, a stop-the-world pause. Cooperative-sticky: only the moved partitions are revoked. Kafka 4.0's new protocol (KIP-848) does assignment on the broker, incrementally.
- **No offsets.** The new owner resumes from the **last committed offset**. Anything the old consumer processed but didn't commit is processed again: **duplicates**, so ZINCRBY double-counts (at-least-once).
- **Imprecise:** "Kafka sends partitions to the consumer". Kafka *assigns*; consumers *pull*.
- **Missed the restart-specific fix:** **static membership** (`group.instance.id`). A consumer that comes back within the session timeout keeps its partitions, with no rebalance at all. Ideal for rolling deploys and container restarts.

**Strong answer covers:**
1. Coordinator broker + heartbeats; each consumer is a group member with a member ID.
2. Detection: LeaveGroup on graceful close (immediate) vs session timeout on crash (delay → lag) vs max.poll.interval for a stuck loop.
3. Rebalance: coordinator bumps the **generation**; members rejoin; assignment computed (group leader in the classic protocol, broker in KIP-848); eager vs cooperative-sticky pause.
4. Resume: new owner fetches the committed offset from `__consumer_offsets`; uncommitted work is replayed, so duplicates. Zombie commits from the old member are rejected (stale generation).
5. Return: rejoin → second rebalance → back to 4/4/4.
6. Improvements: static membership for restarts; cooperative-sticky to avoid stop-the-world; commit on revoke (in the rebalance callback) to shrink the duplicate window; idempotent writes (dedup) to make duplicates harmless.

**Final answer (say this):**
> "Each consumer heartbeats to the group coordinator, a broker. If the consumer shuts down cleanly it sends LeaveGroup and a rebalance starts at once; if it crashes, the coordinator only notices after the session timeout, 45 seconds by default, and its four partitions build lag in the meantime. The coordinator bumps the group generation, the remaining two consumers rejoin, and the 12 partitions are reassigned 6/6. With the eager protocol everyone pauses during that; with cooperative-sticky only the moved partitions do. The new owners start from the last committed offset, so anything the old consumer processed but hadn't committed is processed again: at-least-once, which double-counts in Redis until we dedup. When the consumer comes back it joins, there's a second rebalance, and we're back to 4/4/4. For planned restarts I'd use static membership, so a consumer that returns within the session timeout keeps its partitions and no rebalance happens."

---

## Q6. Two brokers down

**Question:** Your topic has replication factor 3, `min.insync.replicas=2`, and the producer uses `acks=all`. Two of the three brokers go down. What happens to your producer?

**Your answer (summary):** Each key goes to one partition; each partition has a primary and two secondary/replica partitions. Writes to the primary are synced to the replicas and then the "commit" is confirmed. If a broker crashes, we can read from a secondary.

**Feedback (3/10):** Described replication in general but **didn't answer the question** (what happens with two brokers down). Terminology needs fixing:
- "Primary/secondary partitions" → **leader and follower replicas** of one partition. RF 3 = 3 replicas *in total* (1 leader + 2 followers).
- "Commit confirmed" → the producer gets an **ack**. "Commit" is a consumer word (offsets). A record is "committed" in the log once all in-sync replicas have it (below the high watermark).
- `acks=all` waits for all replicas **in the ISR**, not all replicas. `min.insync.replicas` is the **minimum ISR size** for an `acks=all` write to be accepted. Neither term came up.
- "Read from a secondary": consumers read from the **leader**. On failure a follower that's in the ISR is **elected leader**; you don't switch to reading from the follower yourself.

**Strong answer covers:**
1. With 2 of 3 down, at most 1 replica is alive, so ISR size is 1 < `min.insync.replicas` 2. The leader **rejects `acks=all` writes** with `NOT_ENOUGH_REPLICAS`. Writes stop: Kafka chooses **consistency over availability**, by design.
2. Producer: the error is retriable, so it retries (safe because idempotence is on) until `delivery.timeout.ms` (120 s default), then the delivery callback reports failure. Unsent records sit in the producer buffer; when it fills, `produce()` blocks or raises (BufferError in librdkafka).
3. Reads: consumers can still read committed data from the surviving leader, if one exists for the partition.
4. **This cluster specifically:** each node is broker **and** KRaft controller. 2 of 3 down = **controller quorum lost** (needs a majority, 2 of 3). No leader elections or ISR changes can happen, so partitions whose leader died stay offline, and writes time out instead of failing cleanly. The exact error depends on timing; writes stop either way.
5. Contrast: `acks=1` would keep accepting writes on the lone leader, so data written then is lost if that broker dies before the others come back. `unclean.leader.election.enable=true` would let an out-of-sync replica become leader: available again, but committed data lost.
6. Rule: RF 3 + min.insync 2 tolerates **one** broker failure for writes with zero data loss.

**Final answer (say this):**
> "Each partition has three replicas, a leader and two followers. With acks=all the leader acks only when every in-sync replica has the record, and min.insync.replicas=2 says it refuses the write if fewer than two replicas are in sync. With two brokers down only one replica is left, so the leader rejects writes with NOT_ENOUGH_REPLICAS. The producer retries, safely because it's idempotent, until delivery.timeout.ms, then reports the failure; its buffer fills and produce starts failing. Kafka is choosing consistency over availability. Consumers can still read what was already committed. In my cluster it's even stricter: every node is also a KRaft controller, so losing two of three loses the controller quorum and no leader elections can happen at all. RF 3 with min ISR 2 survives exactly one broker failure for writes without data loss."

---

### Follow-up: what is a KRaft controller, and do reads still work?

- **Controller** = the cluster's metadata brain: which brokers are alive, topics and configs, **who leads each partition, the ISR of each partition**. Brokers move the data; the controller decides who is in charge.
- **KRaft** (Kafka Raft) replaced ZooKeeper. Controller nodes run **Raft** over a metadata log (`__cluster_metadata`): one **active controller** (the Raft leader; node 1 in this cluster), the others are hot standbys. Same idea as the NATS JetStream meta-leader.
- A node can be `broker`, `controller` or both (`process.roles`). Here all 3 nodes are both (**combined mode**), so a dead broker is also a dead controller voter.
- **Quorum** = majority of voters: 3 voters need 2. With 2 down → no active controller → **no metadata changes**: no leader elections, no ISR changes, no topic creation.
- Production usually runs **dedicated controllers** (3 small nodes), so losing brokers doesn't cost quorum.

**Reads with 2 of 3 down (Shashi's point: partly right):**
| Partition's leader was on… | Reads | Writes (acks=all) |
|---|---|---|
| the surviving broker | **yes**, already-committed data (data plane doesn't need the controller) | no (ISR < 2) |
| a dead broker | **no**: needs a new leader, the controller can't elect one, so the partition is offline | no |

With 3 brokers and 12 partitions, about 1/3 of partitions are led by the survivor, so roughly a third stay readable. Extra catch: if the **group coordinator** (leader of the group's `__consumer_offsets` partition) was on a dead broker, offset commits and rebalances fail too.
With only **one** broker down, quorum holds (2 of 3), followers are elected leader within seconds, and reads and writes continue.

### Follow-up: what is a (group) coordinator?

- The **broker that manages one consumer group**: tracks members (heartbeats), runs rebalances (join/sync, generation number), and **stores the group's committed offsets**.
- Which broker? `hash(group.id) % 50` picks a partition of the internal topic **`__consumer_offsets`** (50 partitions by default). The **leader of that partition** is the coordinator. Commits are writes to that partition, so they're replicated like any other data.
- Every broker is coordinator for *some* groups, so the load spreads. Kafka UI showed coordinator = 1 for `bsky-likes-leaderboard`.
- If the coordinator broker dies, another replica of that `__consumer_offsets` partition becomes leader → it becomes the new coordinator (needs the controller for the election). Consumers rediscover it with a `FindCoordinator` request.
- **Controller vs coordinator:** controller = one per cluster, manages cluster metadata (leaders, ISR, topics). Coordinator = one per group (spread across brokers), manages that group's membership and offsets.
- There's also a **transaction coordinator** (same idea, `__transaction_state` topic) for transactional producers.

---

## Q7. ISR

**Question:** What is the ISR, and how does a replica fall out of it and get back in?

**Your answer (summary):** The ISR is the secondary replicas syncing from the primary (not the primary itself); with RF 3 the ISR would be replicas 2 and 3, so 1 or 2 replicas. A replica falls out due to network issues or a crash; when it comes back it knows how far it had acknowledged and replays from the primary.

**Feedback (4/10):** Falling out due to lag or crash and catching up from the leader is right. Gaps:
- **The ISR includes the leader.** ISR = all replicas (leader + followers) that are caught up. Healthy RF 3 → ISR size 3, e.g. `[1,2,3]`. `min.insync.replicas=2` counts the leader: leader + 1 follower. If the ISR excluded the leader, min ISR 2 would need both followers, which is wrong.
- Still "primary/secondary" → **leader/follower**.
- **No criterion for falling out:** `replica.lag.time.max.ms` (30 s default). A follower that hasn't caught up to the leader's log end (or stopped fetching) for that long is removed by the leader. It's time-based, not message-count-based.
- **No criterion for getting back in:** once the follower reaches the leader's log end offset, the leader adds it back.
- **Recovery detail:** the follower doesn't resume from "what it acknowledged". It first **truncates** any divergent tail using the **leader epoch** (records the old leader wrote but that never got committed), then fetches from the leader.
- **Didn't say why the ISR matters**, which is the point of the question.

**Strong answer covers:**
1. Definition: the subset of replicas, leader included, fully caught up with the leader. `kafka-topics --describe` shows `Replicas` vs `Isr`.
2. Out: follower not caught up within `replica.lag.time.max.ms` (slow disk, GC, network, crash) → leader shrinks ISR (recorded by the controller).
3. In: follower restarts, truncates to the leader-epoch boundary, fetches, catches up to the leader's log end → leader expands ISR.
4. Why it matters:
   - `acks=all` waits **only for the ISR**, so a slow follower is dropped instead of stalling producers.
   - **High watermark** = highest offset every ISR member has; consumers only read up to it.
   - Clean leader election picks **only from the ISR**, so no committed data is lost.
   - `min.insync.replicas` stops the ISR from shrinking to just the leader while acks=all still succeeds.

**Final answer (say this):**
> "The ISR is the set of replicas, including the leader, that are fully caught up with the leader. Healthy with RF 3, it's all three. A follower is dropped when it hasn't caught up for replica.lag.time.max.ms, 30 seconds by default, because of a crash, slow disk, GC or network. When it comes back it truncates any uncommitted tail using the leader epoch, fetches from the leader, and rejoins once it reaches the leader's log end. It matters because acks=all waits only for the ISR, the high watermark is what every ISR member has, consumers read only up to it, and only an ISR member can become leader without losing data. min.insync.replicas sets how small the ISR may get before writes are refused."

---

## Q8. Idempotent producer

**Question:** Your producer runs with `enable.idempotence=true`. What problem does that solve, and how does Kafka implement it?

**Your answer (summary):** Didn't know.

**Answer:**
1. **Problem 1, duplicates from retries:** the leader writes the batch, but the ack is lost (timeout, network blip, leader failover). The producer can't tell, so it retries, and the batch is written twice.
2. **Problem 2, reordering from retries:** with several batches in flight, batch 1 fails, batch 2 succeeds, batch 1 is retried and lands after batch 2.
3. **How it works:**
   - At start the broker gives the producer a **Producer ID (PID)** and an epoch.
   - Every batch carries **PID + a sequence number per partition**, incrementing.
   - The partition leader remembers the last sequence numbers per PID (last 5 batches). **Seq already seen → duplicate**: acked but not written again. **Seq gap → out-of-order error**, so order holds.
   - That state is stored with the log, so it survives a leader failover.
   - Requires `acks=all`, retries > 0, `max.in.flight.requests.per.connection` ≤ 5 (ordering kept even with 5 in flight).
4. **Limits (where interviews dig):**
   - Scope is **one producer session, per partition**. A producer restart gets a new PID, so no dedup across restarts.
   - It removes duplicates caused by **Kafka-level retries only**. If the application sends the same event twice, both are new sequence numbers and both are written. **This project:** after a restart the producer replays from its Jetstream checkpoint, so some likes land in Kafka twice despite idempotence.
   - Not end-to-end exactly-once. For that: **transactions** (`transactional.id`, stable across restarts, epoch fences zombie producers; consumers use `isolation.level=read_committed`). For an external sink like Redis: an **idempotent consumer** (dedup on the event ID, e.g. `SET NX`).
5. Defaults: on by default in the Java client since Kafka 3.0; **off by default in librdkafka** (confluent-kafka), which is why this project sets it explicitly.

**Final answer (say this):**
> "It stops producer retries from creating duplicates or reordering. If the leader writes a batch but the ack is lost, the producer retries; without idempotence that batch is written twice, and with several requests in flight a retried batch can land after a later one. With idempotence the broker gives the producer an ID, and every batch carries a per-partition sequence number. The leader tracks the last sequence per producer, drops a repeat as a duplicate and rejects a gap, so I get no duplicates and kept order with up to 5 requests in flight. It needs acks=all. The limit is that it only covers retries within one producer session. If my app re-sends an event, like my producer replaying from its Jetstream checkpoint after a restart, those are new sequence numbers and get written again. End to end I need transactions or, with Redis as the sink, an idempotent consumer that dedups on the event ID."

---

## Q9. Delivery semantics in your pipeline

**Question:** End to end, from Bluesky to Redis, what delivery guarantee does your pipeline give today: at-most-once, at-least-once, or exactly-once? Where could a like be lost or counted twice?

**Your answer (summary):** At-least-once (first said only the label; after the follow-up push:) The producer persists a cursor; if the marker isn't written, it re-reads from the last saved point, so at-least-once from Bluesky. Kafka is durable. Redis fsyncs AOF every second, so up to 1 s of likes can be lost on a Redis crash. Consumer crash after the Redis write but before the commit → replay → counted twice.

**Feedback (7/10):** Good hop-by-hop walk, and the Redis AOF point is a strong catch most candidates miss. Gaps:
- Make the producer duplicate explicit: the checkpoint trails what was acked, so after a restart **already-published likes go into Kafka again** (idempotence doesn't catch it; see Q8).
- **Rebalance** also causes duplicates, not only crashes: partitions revoked mid-batch, so the new owner reprocesses from the last commit.
- **Sharpen the Redis point:** the offsets for that lost second were *already committed*, so Kafka will never replay them. That leg is effectively **at-most-once**, so the pipeline is "at-least-once with one at-most-once hole".
- Other loss points: producer outage longer than **Jetstream's replay window** (gap can't be re-read); consumers down longer than **Kafka retention** (24 h here); unclean leader election (off by default); a producer delivery failure after `delivery.timeout.ms`, depending on how the code handles the callback.
- Deliberate drops, worth naming so they aren't mistaken for bugs: events older than 60 min skipped, malformed events skipped (no DLQ yet), closed buckets trimmed to top 100.
- Vocabulary: "queue" → log, "acknowledge" → **commit**.

**Final answer (say this):**
> "At-least-once, with one hole. Hop by hop: the producer checkpoints the Jetstream cursor only up to what Kafka has acked, so after a restart it replays a little and some likes are published twice. Idempotence doesn't catch that, because they're new sends. Kafka with acks=all, RF 3 and min ISR 2 loses nothing on a single broker failure; loss would need consumers down longer than the 24 h retention or an unclean election. The consumer commits after the Redis write, so a crash or rebalance between write and commit replays the batch and double-counts. The hole is Redis: AOF fsyncs every second, and those offsets are already committed, so a Redis crash loses up to a second of counts that Kafka will never replay. And there are deliberate drops: events older than 60 minutes, malformed events, and trimming closed buckets to the top 100."

---

## Q10. Exactly-once into Redis

**Question:** Your interviewer says: "I need exact counts, no duplicates and no losses." How would you change the consumer to get effectively exactly-once into Redis?

**Your answer (summary):** Idempotency at several levels. At the count level: an idempotency key of post ID + user ID; in Redis, transactionally take an "exclusive lock" on that key and increment only if it's new, so one user's like counts once. TTL around 5 minutes; later duplicates are unlikely and can be handled separately.

**Feedback (5/10):** Dedup via an idempotency key in Redis is the right direction. Gaps:
- **Losses not addressed.** The question said "no losses"; the Redis AOF hole from Q9 is still open. Dedup fixes duplicates only.
- **Not a lock, a dedup marker:** `SET key 1 NX EX ttl`. A lock gets released; this marker must stay for the whole TTL.
- **Atomicity unspecified.** SET NX and ZINCRBY must be one atomic step. MULTI/EXEC can't branch on the SET NX result, so use a **Lua script**. Otherwise a crash between them sets the marker without counting: a lost like.
- **TTL far too short.** Duplicates come from replays: a consumer crash (seconds), producer checkpoint replay (minutes), an offset reset (hours). TTL must cover the replay window. Natural choice: **≈65 min, the bucket lifetime** (events older than 60 min are skipped anyway).
- **Key choice:** user+post is *business* dedup (one like per user per post; a re-like after an unlike is dropped). For *delivery* dedup use the like's unique ID (its record URI / rkey). Say which one you mean.
- **Cost at scale:** one marker per like. 195/s × 3,900 s ≈ 760k keys (~100 MB). At 50k/s ≈ 195M keys (~20 GB). This is what pushes towards offsets-in-sink.

**Strong answer covers:**
1. "Exactly-once" in practice = **at-least-once delivery + idempotent processing** ("effectively once").
2. **Consumer-side duplicates and Redis loss: store offsets in the sink.** In the same MULTI/EXEC as the ZINCRBYs, write `offset:{topic}:{partition}` to Redis. On partition assignment, seek to the offsets stored in Redis (not Kafka's). Counts and offsets are now always consistent: a Redis crash that loses 1 s loses *both*, and the consumer re-reads that second from Kafka. A batch whose end offset is already stored is skipped. Kafka commits become informational (lag monitoring).
3. **Producer-side duplicates** (the same like at two offsets) still need **event-ID dedup**: Lua `SET NX EX 3900` + ZINCRBY atomically, keyed by the like's unique ID.
4. Trade-offs: dedup keys cost memory (at scale use a time-bucketed set per minute, or a Bloom filter if approximate is fine); `appendfsync always` closes the loss hole but costs write throughput.
5. Alternatives: Kafka Streams with exactly-once (state and offsets committed in one Kafka transaction), or a sink with transactions (Postgres: upsert counts + offsets in one transaction).

**Final answer (say this):**
> "Exactly-once in practice means at-least-once delivery plus idempotent processing. Two changes. First, I store the Kafka offsets in Redis, in the same MULTI/EXEC as the counter updates, and on partition assignment I seek to those offsets instead of Kafka's committed ones. Counts and offsets can then never disagree: a consumer crash replays nothing already applied, and if Redis loses its last second, it loses the offsets too, so I simply re-read that second from Kafka. That closes both the duplicate and the loss hole on the consumer side. Second, the producer can still publish the same like twice at different offsets after a checkpoint replay, so I dedup on the like's unique ID with a Lua script: SET NX with a TTL of about 65 minutes, matching the bucket lifetime, and ZINCRBY only if it was new. The cost is one key per like, fine at 200 per second; at 50k per second I'd use per-minute dedup sets or accept a Bloom filter's approximation."

---

## Q11. Kafka transactions

**Question:** Kafka has its own exactly-once feature (transactions). When does it give you exactly-once, and why doesn't it solve the Redis problem by itself?

**Your answer (summary):** In the consumer we have an "outbox pattern" situation: writing to Redis and acknowledging to the queue are two systems, so they can't be atomic. Fix: store the commit offset in Redis with the count so the consumer can reconcile on replay.

**Feedback (4/10):** The core "why" is right: two systems, no shared transaction. Gaps:
- **Didn't answer "when does it give exactly-once"**, which is half the question, and you didn't explain how transactions work.
- **Wrong pattern name.** **Outbox** is the *producer-side* problem: write to your DB *and* publish an event. Fix: write the event to an outbox table in the same DB transaction, and a relay (or CDC/Debezium) publishes it. What you described is **storing offsets in the sink** (a transactional or idempotent sink).
- Repeated the Q10 fix instead of answering about Kafka transactions.
- "Acknowledging to the queue" → **committing offsets**.

**Strong answer covers:**
1. **Scope: Kafka → Kafka (read-process-write).** Consume from topic A, produce results to topic B, and commit the input offsets, all in **one Kafka transaction**.
2. **How:**
   - Producer gets a `transactional.id` (stable across restarts) → `initTransactions()`; the **transaction coordinator** (leader of a `__transaction_state` partition) bumps the **epoch**, which **fences zombie** instances with the same ID.
   - `beginTransaction()` → produce outputs → `sendOffsetsToTransaction()` (the input offsets go to `__consumer_offsets` *inside* the transaction) → `commitTransaction()`.
   - The coordinator writes **commit/abort markers** to every partition involved.
   - Downstream consumers use `isolation.level=read_committed`: they read only up to the **last stable offset** and skip aborted records.
   - Kafka Streams wraps all of this: `processing.guarantee=exactly_once_v2`.
3. **Why not Redis:** the transaction covers only Kafka partitions (output topics + offsets). Redis can't join it; there's no two-phase commit with external systems. A crash after the Redis write but before the Kafka commit still replays the batch.
4. **Bridging options:** offsets stored in the sink (Q10); idempotent writes; or keep everything in Kafka (Kafka Streams aggregates with EOS into an output topic) and let Redis be a rebuildable cache fed by an idempotent upsert of *absolute* counts (SET, not INCR).
5. **Costs:** extra latency (outputs visible only after commit, so commit interval matters), coordinator round trips, and read_committed consumers held back by open transactions.

**Final answer (say this):**
> "Kafka transactions give exactly-once for read-process-write *within Kafka*. The app has a transactional.id; the transaction coordinator fences any zombie with the same ID by bumping the epoch. In one transaction it consumes from the input topic, produces to the output topic, and commits the input offsets with sendOffsetsToTransaction. Then the coordinator writes commit markers, and downstream consumers with read_committed only see committed results. Kafka Streams does this with exactly_once_v2. It doesn't solve Redis, because Redis can't take part in a Kafka transaction; there's no distributed commit across the two, so a crash between the Redis write and the offset commit still replays. For an external sink I either store the offsets in the sink atomically with the data, or make the writes idempotent. Or I do the aggregation in Kafka Streams with exactly-once and push absolute counts to Redis with SET, which is idempotent by nature. Different problem: the outbox pattern solves the reverse direction, DB write plus event publish."

---

## Q12. Retention vs log compaction

**Question:** What is log compaction, and when would you use it instead of time-based retention?

**Your answer (summary):** _pending_

**Feedback:** _pending_

**Strong answer covers:** _pending_
