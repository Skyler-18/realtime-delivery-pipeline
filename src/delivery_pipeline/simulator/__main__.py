"""Event simulator: runs the World and publishes its events to Kafka.

Run: uv run python -m delivery_pipeline.simulator
"""

import json
import logging
import random
import time
import uuid
from datetime import UTC, datetime

from delivery_pipeline.config import DRIVER_LOCATION_TOPIC, ORDER_EVENTS_TOPIC, SimulatorSettings
from delivery_pipeline.kafka_utils import ReliableProducer
from delivery_pipeline.lifecycle import install_shutdown_handler
from delivery_pipeline.logging_setup import setup_logging
from delivery_pipeline.simulator.world import World

log = logging.getLogger("simulator")

STATS_INTERVAL_S = 30


def malformed_payload(rng: random.Random, now: datetime) -> tuple[str, bytes]:
    """Deliberately broken driver-location events, to exercise consumers' dead-letter handling."""
    event = {
        "schema_version": 1,
        "event_id": uuid.uuid4().hex,
        "ts": now.isoformat(),
        "driver_id": f"D{rng.randint(1, 9999):04d}",
        "lat": 12.97,
        "lon": 77.59,
        "speed_kmph": 20.0,
        "heading_deg": 90.0,
        "status": "idle",
    }
    kind = rng.choice(["not_json", "missing_field", "out_of_range", "unknown_version"])
    match kind:
        case "not_json":
            return kind, b'{"driver_id": "D0001", "lat": 12.97, '  # truncated JSON
        case "missing_field":
            del event["driver_id"]
        case "out_of_range":
            event["lat"] = 91.5
        case "unknown_version":
            event["schema_version"] = 2
    return kind, json.dumps(event).encode()


def main() -> None:
    setup_logging("simulator")
    settings = SimulatorSettings()
    rng = random.Random(settings.seed)
    world = World(settings, rng)
    producer = ReliableProducer("simulator")
    stop = install_shutdown_handler()
    log.info("Simulating %d drivers, tick=%.1fs, %.1f orders/min", settings.num_drivers, settings.tick_seconds, settings.orders_per_minute)

    sent = malformed = 0
    last_stats = time.monotonic()
    while not stop.is_set():
        tick_started = time.monotonic()
        now = datetime.now(UTC)
        locations, order_events = world.step(now)

        # Key = entity id -> all events of one order / one driver land in one partition, in order.
        for ev in order_events:
            producer.produce(ORDER_EVENTS_TOPIC, ev.order_id, ev.to_bytes())
        for ev in locations:
            producer.produce(DRIVER_LOCATION_TOPIC, ev.driver_id, ev.to_bytes())
            if rng.random() < settings.malformed_rate:
                kind, payload = malformed_payload(rng, now)
                producer.produce(DRIVER_LOCATION_TOPIC, ev.driver_id, payload)
                malformed += 1
                log.debug("Injected malformed event (%s)", kind)
        sent += len(order_events) + len(locations)

        if tick_started - last_stats >= STATS_INTERVAL_S:
            log.info("sent=%d malformed=%d failed=%d world=%s", sent, malformed, producer.failed_deliveries, world.stats())
            last_stats = tick_started

        # Sleep for the rest of the tick (wakes up immediately on shutdown).
        stop.wait(max(0.0, settings.tick_seconds - (time.monotonic() - tick_started)))

    log.info("Flushing producer...")
    producer.flush(10)


if __name__ == "__main__":
    main()
