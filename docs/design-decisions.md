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
