# realtime-delivery-pipeline

Real-time delivery tracking and analytics platform: Kafka (KRaft, 3 brokers) → stream consumers (live map, alerts) → Spark Structured Streaming medallion lakehouse (Bronze/Silver/Gold).

> Work in progress. Full README (architecture diagram, runbook, cost notes) comes in Phase 6.

## Quick start

Prerequisites: Docker Desktop, [uv](https://docs.astral.sh/uv/) (only for running code outside Docker).

```bash
docker compose up -d --build     # Kafka x3, topic init, Kafka UI, simulator, alerts, live-map
```

| What | Where |
|---|---|
| Live map | http://localhost:8000 |
| Kafka UI (topics, consumer lag, dead-letter) | http://localhost:8080 |
| Service logs | `docker compose logs -f simulator alerts live-map` |

Phase 1 smoke test (hello-world producer/consumer):

```bash
uv sync
uv run python -m delivery_pipeline.hello.consumer   # terminal 1
uv run python -m delivery_pipeline.hello.producer   # terminal 2
```

See [docs/design-decisions.md](docs/design-decisions.md) for why things are configured the way they are.
