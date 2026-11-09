# realtime-delivery-pipeline

Real-time delivery tracking and analytics platform: Kafka (KRaft, 3 brokers) → stream consumers (live map, alerts) → Spark Structured Streaming medallion lakehouse (Bronze/Silver/Gold).

> Work in progress. Full README (architecture diagram, runbook, cost notes) comes in Phase 6.

## Quick start (Phase 1)

Prerequisites: Docker Desktop, [uv](https://docs.astral.sh/uv/).

```bash
docker compose up -d                    # 3 Kafka brokers + topic init + Kafka UI
uv sync                                 # create .venv and install deps
uv run python -m delivery_pipeline.hello.consumer   # terminal 1
uv run python -m delivery_pipeline.hello.producer   # terminal 2
```

Kafka UI: http://localhost:8080

See [docs/design-decisions.md](docs/design-decisions.md) for why things are configured the way they are.
