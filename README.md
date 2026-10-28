# Kafka Learn — My Complete Kafka Notes

> Personal notes built around this repo (`kafkajs` + Node.js).
> Read top to bottom the first time. Before an interview, jump to the
> [Revision Cheat Sheet](#15-revision-cheat-sheet) and the [Interview Q&A](#16-interview-questions--answers).

---

## Table of Contents

1. [What is Kafka and why use it](#1-what-is-kafka-and-why-use-it)
2. [Core vocabulary](#2-core-vocabulary)
3. [Architecture: brokers, cluster, KRaft / ZooKeeper](#3-architecture-brokers-cluster-kraft--zookeeper)
4. [Topics, partitions and offsets](#4-topics-partitions-and-offsets)
5. [Replication, leaders, followers and ISR](#5-replication-leaders-followers-and-isr)
6. [Producer — how messages are written](#6-producer--how-messages-are-written)
7. [Consumer — how messages are read](#7-consumer--how-messages-are-read)
8. [Consumer groups and how load is distributed](#8-consumer-groups-and-how-load-is-distributed)
9. [Rebalancing](#9-rebalancing)
10. [Offsets and commits](#10-offsets-and-commits)
11. [Delivery guarantees](#11-delivery-guarantees)
12. [Retention and log compaction](#12-retention-and-log-compaction)
13. [This project — code walkthrough](#13-this-project--code-walkthrough)
14. [How to run it + experiments](#14-how-to-run-it--experiments)
15. [Revision cheat sheet](#15-revision-cheat-sheet)
16. [Interview questions & answers](#16-interview-questions--answers)
17. [Common mistakes / gotchas](#17-common-mistakes--gotchas)

---

## 1. What is Kafka and why use it

**Apache Kafka** is a **distributed event streaming platform**. Think of it as a
very fast, durable, append-only **log** that many applications can write to and
read from at the same time.

It was built at LinkedIn and is now an Apache project.

### The problem it solves

Without Kafka, services talk directly to each other:

```
Service A ──► Service B
Service A ──► Service C
Service D ──► Service B
...  (N × M connections, tight coupling, if B is down A fails)
```

With Kafka, everyone talks to Kafka:

```
Producers                 Kafka                    Consumers
Service A ──►                                  ──► Service B
Service D ──►   [ topic: rider-updt ]          ──► Service C
                                               ──► Analytics
```

### Why Kafka (key properties)

| Property | Meaning |
|---|---|
| **Decoupling** | Producers don't know who consumes. Consumers don't know who produced. |
| **High throughput** | Millions of messages/sec — sequential disk writes, batching, compression, zero-copy. |
| **Durability** | Messages are written to disk and replicated across brokers. |
| **Scalability** | Add partitions and brokers to scale horizontally. |
| **Replayability** | Messages are *not deleted when read*. Consumers can re-read from any offset. |
| **Ordering** | Guaranteed **within a partition**. |
| **Fault tolerance** | Replicas take over if a broker dies. |

### Kafka vs a traditional message queue (RabbitMQ etc.)

| | Kafka | Traditional queue |
|---|---|---|
| Message after read | Stays (until retention expires) | Usually deleted after ack |
| Model | Pull (consumer polls) | Often push |
| Multiple independent readers | Yes, via consumer groups | Need fan-out exchanges / copies |
| Replay old data | Yes | Generally no |
| Ordering | Per partition | Per queue |
| Best for | Event streaming, logs, high volume, analytics | Task queues, complex routing |

### Real-world use cases

- Real-time location tracking (like **this project**: rider location updates — Uber/Zomato/Swiggy style)
- Activity tracking / clickstreams
- Log aggregation
- Metrics and monitoring pipelines
- Event sourcing / CQRS
- Change Data Capture (DB → Kafka → other systems)
- Stream processing (Kafka Streams, Flink)

---

## 2. Core vocabulary

| Term | One-line definition |
|---|---|
| **Message / Record / Event** | A unit of data: `key`, `value`, `headers`, `timestamp`, `partition`, `offset`. |
| **Topic** | A named category/feed of messages, e.g. `rider-updt`. Like a table name. |
| **Partition** | A topic is split into partitions. Each partition is an ordered, append-only log. Unit of parallelism. |
| **Offset** | Sequential ID of a message *within a partition* (0, 1, 2…). |
| **Broker** | A single Kafka server. Stores partitions. |
| **Cluster** | A group of brokers working together. |
| **Producer** | Client that **writes** messages to topics. |
| **Consumer** | Client that **reads** messages from topics. |
| **Consumer Group** | Set of consumers sharing a `groupId` that split a topic's partitions among themselves. |
| **Replication Factor** | How many copies of each partition exist across brokers. |
| **Leader** | The replica of a partition that handles all reads/writes. |
| **Follower** | Replica that copies the leader. Takes over if leader fails. |
| **ISR (In-Sync Replicas)** | Replicas that are fully caught up with the leader. |
| **Group Coordinator** | A broker that manages a consumer group (membership, rebalances, offsets). |
| **Admin client** | Client used for management: create/delete topics, list groups, etc. |
| **Retention** | How long Kafka keeps messages (time or size based). |
| **KRaft** | Kafka's built-in consensus (Raft) that replaced ZooKeeper. |

---

## 3. Architecture: brokers, cluster, KRaft / ZooKeeper

```
                        Kafka Cluster
   ┌─────────────────────────────────────────────────────┐
   │   Broker 1            Broker 2            Broker 3  │
   │  ┌─────────┐        ┌─────────┐        ┌─────────┐  │
   │  │ T-P0 (L)│        │ T-P0 (F)│        │ T-P1 (L)│  │
   │  │ T-P1 (F)│        │ T-P2 (L)│        │ T-P2 (F)│  │
   │  └─────────┘        └─────────┘        └─────────┘  │
   │          (L) = leader replica, (F) = follower       │
   │                                                     │
   │   Controller (KRaft quorum) — metadata & elections  │
   └─────────────────────────────────────────────────────┘
        ▲                                         │
        │ produce                                 │ fetch
   Producers                                  Consumers
```

- **Broker**: a Kafka server process. Identified by a broker id. Our client connects to `localhost:9092`.
- **Bootstrap servers**: the list you give the client (`brokers: ["localhost:9092"]`). The client connects to *any* one, fetches **metadata** (which broker leads which partition), then talks directly to the right brokers. You don't need to list every broker.
- **Controller**: one broker (or a dedicated controller quorum) that manages cluster metadata — topic creation, partition leadership, broker failures.

### ZooKeeper vs KRaft

| | ZooKeeper mode (old) | KRaft mode (new) |
|---|---|---|
| Metadata stored in | External ZooKeeper ensemble | Inside Kafka itself (internal `__cluster_metadata` topic) |
| Extra system to run | Yes | No |
| Status | Deprecated; **removed in Kafka 4.0** | Default and only mode from 4.0 |
| Scalability | Limited (~200k partitions) | Millions of partitions, faster failover |

> Interview line: *"Kafka used ZooKeeper for metadata and controller election. KRaft (KIP-500) moves that into Kafka using the Raft consensus protocol, removing the ZooKeeper dependency."*

---

## 4. Topics, partitions and offsets

### Topic

A logical stream of related events. In this project: `rider-updt` (rider location updates).

### Partition

A topic is divided into **partitions**. Each partition is an **ordered, immutable, append-only log**.

```
Topic: rider-updt  (numPartitions: 2)

Partition 0 (north):  [0][1][2][3][4][5] ──► new messages appended here
Partition 1 (south):  [0][1][2][3]       ──►
                       ▲
                     offsets
```

Why partitions?

1. **Parallelism** — different consumers can read different partitions at the same time.
2. **Scalability** — partitions of one topic can live on different brokers.
3. **Ordering** — ordering is guaranteed **only inside one partition**, *not across the whole topic*.

> **Golden rule:** Max useful consumers in one group = number of partitions.

### Offset

- An integer that uniquely identifies a message **within a partition**.
- Offsets are per partition: partition 0 and partition 1 both have an offset `0`.
- A message is uniquely identified by **(topic, partition, offset)**.
- Offsets only increase; messages are never modified in place.

### Under the hood: segments

Each partition on disk is a directory of **segment files** (`.log` data + `.index` + `.timeindex`).
The active segment receives writes; old segments are deleted/compacted based on retention. This is why Kafka is fast — it does **sequential disk I/O** and relies on the OS page cache.

### Choosing number of partitions

- More partitions → more parallelism, but more open files, longer leader elections, more memory.
- You can **increase** partitions later but **never decrease** them.
- Increasing partitions **changes key → partition mapping** (hash % N changes), which breaks per-key ordering for existing keys.

---

## 5. Replication, leaders, followers and ISR

Each partition has `replication.factor` copies spread across brokers.

```
Partition 0, replication factor = 3

Broker 1: P0  LEADER   ◄── producers write here, consumers read here
Broker 2: P0  follower ── pulls from leader
Broker 3: P0  follower ── pulls from leader
```

- **Leader**: handles all produce and (by default) fetch requests for that partition.
- **Followers**: replicate the leader's log. They don't serve clients (by default).
- **ISR**: the set of replicas that are caught up (within `replica.lag.time.max.ms`).
- If the leader dies, the controller elects a **new leader from the ISR**.
- `min.insync.replicas` (e.g. 2): with `acks=all`, a write succeeds only if at least this many ISR replicas have it. Otherwise producer gets `NotEnoughReplicas`.
- **Unclean leader election** (`unclean.leader.election.enable`): allow an out-of-sync replica to become leader → availability over consistency, **may lose data**. Default `false`.

Typical production setup: `replication.factor=3`, `min.insync.replicas=2`, `acks=all` → survives 1 broker failure with no data loss.

**High Watermark (HW)**: the offset up to which all ISR replicas have the data. Consumers can read only up to the HW (committed messages).

> In this project we run a single local broker, so replication factor is 1 (no replicas).

---

## 6. Producer — how messages are written

A **producer** is a client that publishes messages to a topic.

### Flow of a send

```
producer.send({topic, messages})
        │
        ▼
 1. Serialize key & value  (we do JSON.stringify ourselves)
        │
        ▼
 2. Decide partition:
      - partition given explicitly? → use it        (our producer.js does this)
      - else key present?           → hash(key) % numPartitions
      - else no key                 → sticky / round-robin partitioner
        │
        ▼
 3. Add to batch for that partition (batching + optional compression)
        │
        ▼
 4. Send batch to the LEADER broker of that partition
        │
        ▼
 5. Broker appends to log, replicates, sends ACK based on `acks`
        │
        ▼
 6. Producer gets metadata back: partition + offset (or retries on error)
```

### Message structure

```js
{
  key: "location-update",          // optional; used for partitioning
  value: '{"name":"manish","location":"north"}', // the payload (string/Buffer)
  partition: 0,                     // optional; overrides partitioner
  headers: { source: "app" },       // optional metadata
  timestamp: "1730000000000"        // optional
}
```

### Partitioning strategies

| Case | Which partition |
|---|---|
| `partition` set explicitly | Exactly that partition (manual routing). |
| `key` set | `hash(key) % numPartitions` → **same key always goes to same partition** → per-key ordering. kafkajs uses murmur2 (Java-compatible default partitioner). |
| No key | Spread across partitions (round-robin / sticky batching). No ordering guarantee across messages. |
| Custom partitioner | You write a function `({topic, partitionMetadata, message}) => partitionNumber`. |

**Why keys matter:** if you want all updates for rider "manish" to be in order, use `key: riderName`. All of manish's updates go to one partition, and one partition is read in order by one consumer.

### `acks` — durability vs latency

| `acks` | Meaning | Durability | Latency |
|---|---|---|---|
| `0` | Don't wait for broker at all | Can lose messages | Lowest |
| `1` | Wait for **leader** only | Lost if leader dies before followers copy | Medium |
| `all` / `-1` | Wait for **all ISR** | Strongest (with `min.insync.replicas ≥ 2`) | Highest |

In kafkajs: `producer.send({ topic, messages, acks: -1 })` (default is `-1` = all).

### Other important producer concepts

- **Batching**: messages to the same partition are grouped (`batch.size`, `linger.ms` in Java). Bigger batches = better throughput.
- **Compression**: `gzip`, `snappy`, `lz4`, `zstd`. Whole batches are compressed. kafkajs: `compression: CompressionTypes.GZIP`.
- **Retries**: on transient failure the producer retries. Retries can cause **duplicates** (broker wrote it but ack was lost).
- **Idempotent producer** (`idempotent: true`): broker de-duplicates using producer id + sequence number → no duplicates from retries, ordering preserved per partition.
- **Transactions**: write to multiple partitions/topics atomically (all or nothing). Basis for **exactly-once** in Kafka.
- **`sendBatch`**: kafkajs method to send to multiple topics in one call.

---

## 7. Consumer — how messages are read

A **consumer** reads messages from one or more topics.

### Key points

- Kafka consumers **pull** data (`fetch` requests). The broker does not push.
  Pull lets each consumer go at its own pace (natural back-pressure).
- A consumer reads a partition **in offset order**.
- Reading does **not delete** the message. Other consumer groups can read it too.
- Each consumer belongs to a **consumer group** (`groupId`).
- Progress is saved as **committed offsets** per (group, topic, partition) in the internal topic `__consumer_offsets`.

### Flow

```
consumer.connect()
      │
consumer.subscribe({ topic, fromBeginning })
      │
consumer.run({ eachMessage })
      │
      ├─► join group → coordinator assigns partitions (rebalance)
      ├─► for each assigned partition: find starting offset
      │       committed offset exists? → start from it
      │       else → use fromBeginning (earliest) or latest
      ├─► fetch loop → call eachMessage for every message
      ├─► send heartbeats so coordinator knows we're alive
      └─► commit offsets (auto by default in kafkajs)
```

### `fromBeginning` (a.k.a. `auto.offset.reset`)

Applies **only when the group has no committed offset** for a partition:

| Setting | Behaviour |
|---|---|
| `fromBeginning: true` (`earliest`) | Start from the oldest message still retained. |
| `fromBeginning: false` (`latest`) | Only get messages produced after the consumer joins. |

> Gotcha: if group `user-1` has already committed offsets, restarting it with `fromBeginning: true` does **not** replay old messages. It continues from the committed offset. To replay, use a **new groupId** or reset offsets (`admin.resetOffsets` / `consumer.seek`).

### `eachMessage` vs `eachBatch`

- `eachMessage`: simple, one message at a time (used in our `consumer.js`).
- `eachBatch`: get a whole batch; more control (manual `resolveOffset`, `heartbeat`, `commitOffsetsIfNecessary`). Better for high throughput.

### Callback arguments (our code)

```js
eachMessage: async ({ topic, partition, message, heartbeat, pause }) => { ... }
```

| Arg | Meaning |
|---|---|
| `topic` | Topic name |
| `partition` | Partition number the message came from |
| `message` | `{ key, value, offset, timestamp, headers }` — `key`/`value` are **Buffers**, hence `.toString()` |
| `heartbeat()` | Manually send a heartbeat during long processing to avoid being kicked from the group |
| `pause()` | Pause fetching this topic-partition (returns a resume function) — for back-pressure |

---

## 8. Consumer groups and how load is distributed

**This is the most asked topic in interviews.**

### Rule

> Within **one consumer group**, **each partition is assigned to exactly one consumer**.
> One consumer may handle **many** partitions.
> **Different groups** each get **all** the messages independently.

So:
- **Same groupId** → consumers **share** the work (queue / load balancing behaviour).
- **Different groupId** → each group gets **a full copy** of the stream (pub-sub / broadcast behaviour).

Kafka gives you **both queue and pub-sub** using this single mechanism.

### Scenarios with our topic `rider-updt` (2 partitions)

#### Case 1 — One group, ONE consumer

```
               ┌──────────── group: user-1 ────────────┐
P0 (north) ───►│ consumer A  (gets P0 and P1)          │
P1 (south) ───►│                                       │
               └───────────────────────────────────────┘
```
Consumer A receives **everything**.

#### Case 2 — One group, TWO consumers (even distribution)

```
               ┌──────────── group: user-1 ────────────┐
P0 (north) ───►│ consumer A  (only P0 → north riders)  │
P1 (south) ───►│ consumer B  (only P1 → south riders)  │
               └───────────────────────────────────────┘
```
Each consumer gets **one partition**. Load is split evenly. A message is processed by **only one** of them — never both.

In this project, since producer routes `north → P0` and `south → P1`, one terminal will print only north updates and the other only south updates.

#### Case 3 — One group, THREE consumers (more consumers than partitions)

```
               ┌──────────── group: user-1 ────────────┐
P0 (north) ───►│ consumer A  (P0)                      │
P1 (south) ───►│ consumer B  (P1)                      │
               │ consumer C  (IDLE — nothing assigned) │
               └───────────────────────────────────────┘
```
Consumer C sits **idle**. It is a hot standby: if A or B dies, a rebalance gives C its partition.

> **Consumers beyond partition count are idle.** To scale out further, increase partitions.

#### Case 4 — Fewer consumers than partitions (e.g. 6 partitions, 4 consumers)

```
consumer A: P0, P1
consumer B: P2, P3
consumer C: P4
consumer D: P5
```
Distribution is as even as possible, but some consumers get more partitions.

#### Case 5 — Two DIFFERENT groups

```
                ┌─── group: user-1 ───┐
P0 ───┬────────►│ consumer A (P0, P1) │
      │         └─────────────────────┘
P1 ───┼──┐      ┌─── group: user-2 ───┐
      └──┼─────►│ consumer X (P0)     │
         └─────►│ consumer Y (P1)     │
                └─────────────────────┘
```
**Both groups receive every message.** Each group tracks its own offsets. Example: `notification-service` group and `analytics-service` group both reading `rider-updt`.

### Summary table (topic with N partitions, C consumers in one group)

| Situation | Result |
|---|---|
| C = 1 | One consumer reads all N partitions |
| C < N | Each consumer gets ≥ 1 partition, some get more |
| C = N | Perfect 1:1 — ideal parallelism |
| C > N | C − N consumers idle |
| Multiple groups | Each group independently gets all messages |

### How "even" is decided — partition assignment strategies

The group **leader** (first consumer to join; a consumer, not a broker) runs the assignor and sends the plan via the coordinator.

| Strategy | How it assigns |
|---|---|
| **Range** (Java default historically) | Per topic, contiguous ranges: P0-P2 → A, P3-P5 → B. Can be uneven across many topics. |
| **RoundRobin** | Deals partitions one by one across consumers like cards. Evener. **kafkajs default.** |
| **Sticky** | Even like round-robin but tries to **keep existing assignments** across rebalances. |
| **CooperativeSticky** | Sticky + **incremental** rebalancing (doesn't stop everyone). |

kafkajs: `kafka.consumer({ groupId, partitionAssigners: [PartitionAssigners.roundRobin] })`.

---

## 9. Rebalancing

A **rebalance** is the process of redistributing partitions among consumers of a group.

### Triggers

- A consumer **joins** the group (you start another terminal).
- A consumer **leaves** (graceful `disconnect()` or crash).
- A consumer **misses heartbeats** (`sessionTimeout`, default 30s in kafkajs) → considered dead.
- A consumer takes too long between polls (`max.poll.interval.ms` in Java).
- Topic **partitions are added**, or a subscribed pattern matches a new topic.

### What happens (eager protocol)

1. Coordinator notices membership change.
2. **All consumers stop** consuming and give up their partitions ("stop-the-world").
3. Everyone re-joins (`JoinGroup`), leader computes new assignment.
4. `SyncGroup` distributes the assignment.
5. Consumers resume from the **last committed offsets**.

### Problems & solutions

| Problem | Solution |
|---|---|
| Stop-the-world pause | **Cooperative/incremental rebalancing** (only moved partitions pause). New consumer protocol KIP-848 moves assignment to the broker. |
| Rebalance on every restart / deploy | **Static membership** (`group.instance.id`) — a restarted consumer with the same id gets its partitions back without a rebalance (within session timeout). |
| Duplicates after rebalance | Messages processed but not committed get re-delivered to the new owner → make processing **idempotent**. |
| Consumer kicked for slow processing | Call `heartbeat()`, use `eachBatch`, tune `sessionTimeout` / processing time. |

### Heartbeats

Consumers send heartbeats to the **group coordinator** (`heartbeatInterval`, default 3s in kafkajs). If none arrive within `sessionTimeout`, the consumer is removed and a rebalance starts.

---

## 10. Offsets and commits

Committed offset = "the **next** offset this group should read" for a partition. Stored in `__consumer_offsets`.

```
Partition 0:  [0][1][2][3][4][5][6]
                          ▲        ▲
                 committed=4      log end offset = 7
                 (0..3 done)
Consumer lag = 7 - 4 = 3
```

### Commit styles

| Style | How | Risk |
|---|---|---|
| **Auto commit** (kafkajs default `autoCommit: true`) | Commits periodically (`autoCommitInterval`) / every N messages (`autoCommitThreshold`) after `eachMessage` resolves | Crash between processing and commit → duplicates |
| **Manual commit** | `autoCommit: false` + `consumer.commitOffsets([...])` after processing | You control it; more code |
| Commit **before** processing | | Crash → message **lost** (at-most-once) |
| Commit **after** processing | | Crash → message **re-processed** (at-least-once) |

### Consumer lag

`lag = latest offset in partition − committed offset of group`.
The #1 metric for consumer health. Growing lag = consumers can't keep up → add consumers (up to partition count), optimize processing, or add partitions.

Check with CLI:
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group user-1
```

### Seeking / replay

- `consumer.seek({ topic, partition, offset })` — jump to an offset.
- `admin.resetOffsets({ groupId, topic, earliest: true })` — reset a group (group must be inactive).
- Or just use a **new groupId**.

---

## 11. Delivery guarantees

| Semantics | Meaning | How |
|---|---|---|
| **At-most-once** | Never duplicated, may be lost | Commit before processing; `acks=0`; no retries |
| **At-least-once** (most common default) | Never lost, may be duplicated | `acks=all`, retries, commit **after** processing |
| **Exactly-once (EOS)** | Each message affects the result once | Idempotent producer + **transactions** + consumer `isolation.level=read_committed` (read-process-write within Kafka) |

Important nuance: Kafka's exactly-once applies **inside Kafka** (consume → process → produce to Kafka). For external systems (DB, email), you still need **idempotent processing** (e.g. unique message id / upsert / dedupe table).

---

## 12. Retention and log compaction

Kafka doesn't delete messages when they are read. It deletes them by **policy**:

| Config | Meaning |
|---|---|
| `retention.ms` | Keep messages for X time (default 7 days). |
| `retention.bytes` | Keep up to X bytes per partition. |
| `cleanup.policy=delete` | Delete old segments (default). |
| `cleanup.policy=compact` | **Log compaction**: keep only the **latest value per key**. |
| `cleanup.policy=compact,delete` | Both. |

### Log compaction

```
Before:  (manish,north) (rahul,south) (manish,east) (rahul,null)
After:   (manish,east)                              (rahul,null → later removed)
```
- Keeps the last value per key → topic acts like a **key-value snapshot**.
- A message with `value = null` is a **tombstone** → deletes that key.
- Used by `__consumer_offsets`, CDC, "latest state" topics (e.g. latest location of each rider!).

---

## 13. This project — code walkthrough

### Project structure

```
kafka-learn/
├── client.js     # shared Kafka client config
├── admin.js      # creates the topic "rider-updt" with 2 partitions
├── producer.js   # reads "<name> <location>" from terminal and publishes
├── consumer.js   # joins a group (from CLI arg) and prints messages
└── package.json  # dependency: kafkajs
```

Use case: **rider location updates**. Riders in the `north` go to partition 0, everyone else to partition 1.

### `client.js` — the connection

```js
import {Kafka} from "kafkajs";
export const kafka = new Kafka({
    clientId: 'kafka-learn',          // name shown in broker logs/metrics
    brokers: ["localhost:9092"]       // bootstrap broker(s)
})
```
- One `Kafka` instance is shared; from it we create `admin()`, `producer()`, `consumer()`.
- `brokers` = bootstrap servers. The client fetches full cluster metadata from here.

### `admin.js` — create the topic

```js
const admin = kafka.admin();
await admin.connect();
await admin.createTopics({
    topics: [{ topic: "rider-updt", numPartitions: 2 }],
});
await admin.disconnect();
```
- Admin client = management operations (create/delete topics, list groups, fetch offsets, reset offsets).
- `numPartitions: 2` → P0 and P1. This is why at most **2** consumers per group can be active.
- `replicationFactor` not set → broker default (1 on a local single broker).
- Run **once**. Running again: `createTopics` returns `false` if it already exists (no crash).

### `producer.js` — publish rider updates

```js
const producer = kafka.producer();
await producer.connect();

rl.on("line", async function (line) {
  const [riderName, location] = line.split(" ");        // "manish north"
  await producer.send({
    topic: 'rider-updt',
    messages: [{
      partition: location.toLowerCase() === "north" ? 0 : 1,  // manual routing
      key: "location-update",
      value: JSON.stringify({ name: riderName, location }),
    }],
  });
}).on("close", async () => {
  await producer.disconnect();                           // Ctrl+D / Ctrl+C
});
```

What to note:
- Uses Node's `readline` to read a line from the terminal: `manish north`.
- **Explicit `partition`** → our own routing: `north → P0`, anything else → `P1`.
  When `partition` is given, the **key is NOT used for routing**.
- `key: "location-update"` is the same for every message. If we removed `partition`, *all* messages would hash to the **same** partition (same key → same partition) and the other partition would get nothing. Better key would be `riderName` → per-rider ordering + spread across partitions.
- `value` must be a string/Buffer → `JSON.stringify`.
- `producer.send` returns metadata (`[{ topicName, partition, baseOffset, ... }]`) — could log it to see offsets.
- Default `acks` in kafkajs is `-1` (all).

### `consumer.js` — read rider updates in a group

```js
const group = process.argv[2] || "user-1";               // node consumer.js user-2
const consumer = kafka.consumer({ groupId: group });
await consumer.connect();
await consumer.subscribe({ topic: "rider-updt", fromBeginning: true });
await consumer.run({
  eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
    console.log(`${group}: [${topic}]: PART:${partition}:`, message.value.toString());
  },
});
```

What to note:
- **Group id from CLI argument** → easy to experiment with same-group vs different-group consumers.
- `fromBeginning: true` → a *new* group reads all retained history; an existing group continues from its committed offset.
- `message.value` is a Buffer → `.toString()`.
- Auto-commit is on (kafkajs default).
- Output shows the `partition`, so you can *see* the distribution.

Sample output with two consumers in group `user-1`:
```
# terminal 1
user-1: [rider-updt]: PART:0: {"name":"manish","location":"north"}
# terminal 2
user-1: [rider-updt]: PART:1: {"name":"rahul","location":"south"}
```

### End-to-end flow in this project

```
 terminal (producer.js)                 Kafka broker :9092                 terminals (consumer.js)
 "manish north" ──► partition = 0 ──►  rider-updt / P0 [..][..][new] ──►  group user-1 → consumer A
 "rahul south"  ──► partition = 1 ──►  rider-updt / P1 [..][new]     ──►  group user-1 → consumer B
                                                                     └─►  group user-2 → consumer X (gets both)
```

### Possible improvements (good to talk about in interviews)

- Validate input: `"manish"` with no location → `location` is `undefined` → `location.toLowerCase()` throws. Guard: `if (!riderName || !location) return;`.
- Use `key: riderName` (per-rider ordering) or a **custom partitioner** instead of hard-coding partitions.
- Graceful shutdown for the consumer: listen to `SIGINT`/`SIGTERM` and call `consumer.disconnect()` so the group rebalances immediately instead of waiting for the session timeout.
- Add `"type": "module"` to `package.json` since files use `import` (newer Node versions auto-detect ES modules, older ones throw `SyntaxError: Cannot use import statement outside a module`).
- Move broker list to env vars (`.env` is already in `.gitignore`).
- Make the producer idempotent: `kafka.producer({ idempotent: true })`.

---

## 14. How to run it + experiments

### 1. Start Kafka locally (Docker, KRaft single-node)

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.8.0
```

(Older tutorials use ZooKeeper + `confluentinc/cp-kafka`; with KRaft you need only one container.)

### 2. Install dependencies

```bash
yarn install     # or: npm install
```

### 3. Create the topic

```bash
node admin.js
```

### 4. Start consumers (separate terminals)

```bash
node consumer.js user-1
node consumer.js user-1
```

### 5. Start the producer and type messages

```bash
node producer.js
> manish north
> rahul south
> amit north
```

### Experiments to *see* the theory

| # | Do this | Expected result | Concept |
|---|---|---|---|
| 1 | 1 consumer in `user-1` | It prints PART:0 and PART:1 messages | One consumer owns all partitions |
| 2 | 2 consumers in `user-1` | One prints only PART:0 (north), other only PART:1 (south) | Even distribution, 1 partition each |
| 3 | 3 consumers in `user-1` | One consumer prints nothing | Consumers > partitions → idle |
| 4 | Kill one of the 2 consumers | After a few seconds the survivor gets both partitions | Rebalance / failover |
| 5 | Run `user-1` and `user-2` | Both groups print every message | Pub-sub across groups |
| 6 | Restart `user-1` | Doesn't re-print old messages | Committed offsets win over `fromBeginning` |
| 7 | Start new group `user-3` | Prints full history | New group + `fromBeginning: true` |
| 8 | Send many `north` messages | Always same consumer, always in order | Ordering within a partition |

### Useful CLI commands (inside the container: `docker exec -it kafka bash`, scripts in `/opt/kafka/bin`)

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --list
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic rider-updt
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic rider-updt
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic rider-updt --from-beginning --property print.partition=true
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group user-1
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group user-1 --reset-offsets --to-earliest --topic rider-updt --execute
```

---

## 15. Revision cheat sheet

**One-liners to remember**

- Kafka = distributed, durable, partitioned, replicated **commit log**.
- **Topic** → split into **partitions** → each partition = ordered append-only log → each message has an **offset**.
- Message identity = **(topic, partition, offset)**.
- **Ordering** is guaranteed **only within a partition**.
- **Same key → same partition** (`hash(key) % partitions`), unless partition count changes.
- **Producer** writes to the **leader** of a partition. `acks = 0 | 1 | all`.
- **Consumer** **pulls**. Reading doesn't delete.
- **Consumer group**: each partition → exactly **one** consumer in the group.
- Max active consumers in a group = **number of partitions**; extra ones are idle.
- Different groups = independent full copies (pub-sub). Same group = load sharing (queue).
- **Rebalance** happens on join / leave / crash / partition change.
- **Offsets** committed to `__consumer_offsets`. **Lag** = log end − committed.
- `fromBeginning` / `auto.offset.reset` applies **only** if no committed offset.
- **Replication factor** = copies; **ISR** = caught-up replicas; leader elected from ISR.
- Safe config: `RF=3`, `min.insync.replicas=2`, `acks=all`, idempotent producer.
- Semantics: at-most-once / at-least-once (default) / exactly-once (idempotence + transactions + read_committed).
- Retention by time/size; **compaction** keeps the latest value per key; `null` value = tombstone.
- Partitions can be **increased, never decreased**.
- **KRaft** replaced ZooKeeper (ZooKeeper removed in Kafka 4.0).
- Why fast: sequential I/O, OS page cache, batching, compression, zero-copy (`sendfile`).

**kafkajs API quick map**

```js
const kafka = new Kafka({ clientId, brokers });

// admin
const admin = kafka.admin();  await admin.connect();
await admin.createTopics({ topics: [{ topic, numPartitions, replicationFactor }] });
await admin.listTopics();  await admin.fetchTopicOffsets(topic);
await admin.fetchOffsets({ groupId, topics: [topic] });

// producer
const producer = kafka.producer({ idempotent: true });
await producer.connect();
await producer.send({ topic, acks: -1, messages: [{ key, value, partition, headers }] });
await producer.disconnect();

// consumer
const consumer = kafka.consumer({ groupId, sessionTimeout: 30000, heartbeatInterval: 3000 });
await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: true });  // or topics: [...]
await consumer.run({ autoCommit: true, eachMessage: async ({ topic, partition, message }) => {} });
consumer.seek({ topic, partition, offset: '0' });
await consumer.disconnect();
```

**Numbers worth knowing (defaults)**

| Setting | Default |
|---|---|
| Retention | 7 days (`retention.ms = 604800000`) |
| kafkajs `sessionTimeout` | 30 000 ms |
| kafkajs `heartbeatInterval` | 3 000 ms |
| kafkajs `acks` | -1 (all) |
| kafkajs default assigner | RoundRobin |
| Max message size (broker) | ~1 MB (`message.max.bytes`) |
| Default port | 9092 |

---

## 16. Interview questions & answers

**Q1. What is Kafka?**
A distributed event streaming platform: a partitioned, replicated, durable commit log used to publish, store, and process streams of records in real time with high throughput.

**Q2. Topic vs partition?**
Topic is a logical category. A partition is a physical, ordered, append-only log that a topic is split into. Partitions give parallelism and scalability.

**Q3. What is an offset?**
A sequential id of a record within a partition. Consumers track their position using committed offsets.

**Q4. How does Kafka guarantee ordering?**
Only within a partition. To keep order for an entity (e.g. a rider/user/order), use that entity's id as the **key** so all its messages land in the same partition.

**Q5. How does a producer decide the partition?**
Explicit partition if given → else hash of key mod partitions → else round-robin/sticky. Or a custom partitioner.

**Q6. Explain `acks`.**
`0` = fire and forget; `1` = leader acknowledged; `all` = all in-sync replicas acknowledged (with `min.insync.replicas`). Trade-off between latency and durability.

**Q7. What is a consumer group?**
A set of consumers with the same `groupId` that divide a topic's partitions among themselves. Each partition is consumed by exactly one member, so each message is processed once per group.

**Q8. 2 partitions and 3 consumers in the same group — what happens?**
Two consumers get one partition each; the third is idle (standby). It takes over if another fails.

**Q9. 2 consumers in different groups?**
Both receive all messages. Groups are independent and keep separate offsets.

**Q10. How is load evenly distributed?**
The group coordinator (a broker) manages membership; the group leader consumer runs an assignment strategy (Range, RoundRobin, Sticky, CooperativeSticky) to spread partitions as evenly as possible. True balance also depends on even data per partition (good key choice, no hot keys).

**Q11. What is a rebalance and what triggers it?**
Reassignment of partitions within a group. Triggered by consumers joining/leaving/crashing (missed heartbeats or slow polling) or partition/subscription changes. Mitigate with cooperative rebalancing and static membership.

**Q12. Where are consumer offsets stored?**
In the internal compacted topic `__consumer_offsets` (in old versions, ZooKeeper).

**Q13. Auto vs manual commit?**
Auto commits periodically — simple but may cause duplicates or (if commit precedes processing) loss. Manual commit after processing gives at-least-once with control.

**Q14. Explain at-most-once, at-least-once, exactly-once.**
See [section 11](#11-delivery-guarantees). EOS = idempotent producer + transactions + `read_committed` consumers.

**Q15. What is an idempotent producer?**
Producer gets a producer id; each message has a sequence number per partition; broker drops duplicates from retries. Prevents duplicates and preserves order.

**Q16. What is ISR?**
In-Sync Replicas — replicas that are caught up with the leader. Only ISR members are eligible for leader election (unless unclean election is enabled).

**Q17. What happens when a broker dies?**
For partitions it led, the controller elects new leaders from the ISR. Clients refresh metadata and continue with the new leaders. With RF=3 and min ISR=2 no committed data is lost.

**Q18. What is consumer lag and how do you fix high lag?**
Difference between latest offset and committed offset. Fix: add consumers (≤ partitions), add partitions, speed up processing (batching, async I/O), fix hot partitions.

**Q19. Can you decrease partitions?**
No. Only increase, and that changes key → partition mapping.

**Q20. What is log compaction?**
Retention policy that keeps only the latest record for each key; `null` value tombstones delete a key. Good for state/snapshot topics.

**Q21. Why is Kafka so fast?**
Sequential disk writes/reads, OS page cache, batching, compression, zero-copy transfer, partitioned parallelism, simple broker (consumers track their own position).

**Q22. Kafka vs RabbitMQ?**
Kafka: durable replayable log, pull, high throughput, per-partition ordering, multiple independent consumer groups. RabbitMQ: smart broker, push, flexible routing, messages removed after ack, great for task queues.

**Q23. What is ZooKeeper's role / KRaft?**
ZooKeeper stored metadata and did controller election. KRaft puts this inside Kafka with the Raft protocol; ZooKeeper is removed in 4.0.

**Q24. Push or pull?**
Pull. Consumers control their rate, can batch efficiently, and can rewind.

**Q25. How would you handle a message that keeps failing (poison pill)?**
Retry a limited number of times, then send it to a **Dead Letter Queue (DLQ)** topic with error info and continue; don't block the partition forever.

**Q26. How do you guarantee per-user ordering while scaling consumers?**
Key by user id → same partition → only one consumer in the group processes that partition, sequentially.

**Q27. What is a hot partition?**
One partition receiving far more traffic (skewed key, e.g. our constant key `"location-update"`). That consumer becomes the bottleneck. Fix with better keys, key salting, or custom partitioner.

**Q28. In your project, why did two consumers split north and south?**
The topic has 2 partitions; the producer explicitly sends `north` to P0 and others to P1; with two consumers in the same group, each was assigned one partition.

**Q29. What are Kafka Connect and Kafka Streams?**
Connect: framework for source/sink connectors to move data between Kafka and external systems (DBs, S3, Elasticsearch) without code. Streams: Java library for stateful stream processing (filter, map, join, aggregate, windowing) on Kafka topics.

**Q30. What is Schema Registry?**
A service storing Avro/Protobuf/JSON schemas for topics so producers and consumers agree on message format and evolve it compatibly.

---

## 17. Common mistakes / gotchas

- Expecting global ordering across a topic — only per partition.
- More consumers than partitions and wondering why some are idle.
- Same key for all messages → everything in one partition (hot partition).
- Thinking `fromBeginning: true` replays for an existing group — it doesn't.
- Not calling `disconnect()` on shutdown → slow rebalances (waits for session timeout).
- Long processing in `eachMessage` without heartbeats → consumer kicked → rebalance → duplicates.
- Assuming exactly-once by default — default is at-least-once; make consumers idempotent.
- Increasing partitions on a keyed topic and breaking ordering assumptions.
- Forgetting `message.value` is a Buffer (`.toString()` / `JSON.parse`).
- Using `acks=1` / RF=1 in production and losing data on broker failure.
- Not monitoring consumer lag.
