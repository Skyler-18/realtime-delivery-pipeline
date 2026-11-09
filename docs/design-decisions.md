# Design decisions

Living log of *why* things are the way they are. Expanded every phase.

## Kafka cluster (local)

| Decision | Choice | Why |
|---|---|---|
| Metadata mode | KRaft, Kafka 4.0 | ZooKeeper is removed in Kafka 4.x; one system to run instead of two. |
| Node roles | 3 nodes, each broker + controller ("combined") | Smallest setup with a real controller quorum (tolerates 1 failure) and real replication. Production would use dedicated controllers. |
| Replication factor | 3 | Every partition survives the loss of any 2 brokers' disks (data-wise). |
| `min.insync.replicas` | 2 | With `acks=all`, a write is acknowledged only when 2 replicas have it. Cluster keeps accepting writes with 1 broker down, and an acknowledged write is never on only one disk. |
| Auto topic creation | Disabled | Topics are infrastructure: created by `infra/kafka/create-topics.sh` with deliberate settings. A typo in a producer must fail, not silently create a topic with default settings. |
| Listeners | `INTERNAL` (container network), `EXTERNAL` (host), `CONTROLLER` (quorum) | Clients use the *advertised* address returned in metadata; containers and host need different addresses. |

## Topics

| Topic | Partitions | Key | Retention | Reasoning |
|---|---|---|---|---|
| `driver-location` | 6 | `driver_id` | 1 day | Highest volume (every driver every ~2s). Keying by driver keeps each driver's pings ordered. 6 partitions lets up to 6 consumers per group work in parallel. Data is persisted to the lakehouse (Bronze), so Kafka only needs enough retention to replay after an outage. |
| `order-events` | 3 | `order_id` | 7 days | Lower volume, but lifecycle order (placed → assigned → … → delivered) must hold per order, hence key = `order_id`. Longer retention lets us rebuild downstream state/Gold tables. |
| `alerts` | 3 | `driver_id` / `order_id` | 3 days | Derived data, can be regenerated from source topics. |
| `dead-letter` | 1 | original key | 14 days | Tiny volume; a single partition keeps a simple global order for debugging. Long retention so someone has time to inspect and replay bad messages. |
| `hello-world` | 3 | user | 1 hour | Phase 1 smoke test only. |

Note: partitions can be **increased** later but never decreased, and increasing them changes the key → partition mapping (breaks per-key ordering for in-flight keys). So we pick a sensible number up front.

## Events and schemas

| Decision | Choice | Why / trade-off |
|---|---|---|
| Serialization | JSON validated by Pydantic models | Human-readable in Kafka UI, zero extra infrastructure. Production alternative: Avro/Protobuf + Schema Registry (smaller messages, compatibility enforced centrally). |
| Versioning | `schema_version` on every event; unknown version → dead-letter | Consumers never guess the shape of data. |
| Unknown fields | Ignored ("tolerant reader") | Producers can add optional fields without breaking old consumers. Breaking changes need a new version. |
| Denormalized order events | Every order event repeats area, coordinates, amount, `placed_at` | Any single event is self-describing; consumers (alerts, Spark) don't need to join to reconstruct an order. |
| Keys | `driver_id` for locations, `order_id` for orders | Per-entity ordering, and stateful consumers get all events of an entity in one partition. |

## Consumers

| Decision | Choice | Why / trade-off |
|---|---|---|
| Delivery guarantee | At-least-once | Offsets are stored only after a message is handled, and committed only after everything we produced for it is acknowledged (`flush` before `commit`). A crash means re-processing, never loss. |
| Commit cadence | Every 1 s (and on partition revoke / shutdown) | Per-message synchronous commits cost a round trip each. Batching trades up to ~1 s of duplicate processing after a crash for much higher throughput. |
| Duplicates | Alert ids are deterministic (`uuid5` of the rule + input) | Re-processing produces the same alert id, so downstream can de-duplicate. |
| Bad messages | Sent unchanged to `dead-letter` with headers (error, service, source topic/partition/offset) | A poison message must not block a partition forever, nor crash the consumer in a loop. Headers make it debuggable and replayable. Each consumer group dead-letters independently, so one bad message can appear once per service. |
| Bugs / infra errors | Exception → process exits → Docker restarts it | Retrying a bug forever hides it; restarting resumes from the last committed offset. |
| Alerts state | In memory, keyed like the input topics, reset on rebalance | Simple and correct per partition. Limitation: state is lost on restart (e.g. a stationary episode restarts counting). Production: Kafka Streams / Flink state stores backed by a changelog topic. |
| Alerts time | Event time (max `ts` seen), not wall clock | Replays and lagging consumers produce the same, correct alerts. |
| live-map offsets | `auto.offset.reset=latest`, single instance | A live view doesn't need history. Each instance needs ALL drivers, so instances must not share a consumer group. |

## Simulator

- Movement is a straight line with one random bend per leg, not real roads (would need a routing engine such as OSRM with several GB of map data; not the focus of this project).
- Defaults follow Little's law: ~6 orders/min × ~12 min per delivery ≈ 70 of 100 drivers busy.
- Anomalies are injected on purpose: drivers getting stuck (4–8 min), speeding bursts (65–95 km/h), ~1% malformed location events (truncated JSON, missing field, out-of-range coordinates, unknown schema version).
