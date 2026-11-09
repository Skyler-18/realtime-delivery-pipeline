"""Alerts service: consumes driver-location + order-events, publishes to `alerts`.

Run: uv run python -m delivery_pipeline.alerts
"""

import logging

from pydantic import BaseModel

from delivery_pipeline.alerts.rules import AlertEngine
from delivery_pipeline.config import ALERTS_TOPIC, DRIVER_LOCATION_TOPIC, ORDER_EVENTS_TOPIC, AlertSettings
from delivery_pipeline.kafka_utils import ConsumerRunner, ReliableProducer, make_consumer
from delivery_pipeline.lifecycle import install_shutdown_handler
from delivery_pipeline.logging_setup import setup_logging
from delivery_pipeline.schemas import DriverLocation, OrderEvent

SERVICE = "alerts-service"
log = logging.getLogger("alerts")


def main() -> None:
    setup_logging("alerts")
    engine = AlertEngine(AlertSettings())
    producer = ReliableProducer(SERVICE)

    def handle(event: BaseModel) -> None:
        for alert in engine.process(event):
            producer.produce(ALERTS_TOPIC, alert.driver_id or alert.order_id, alert.to_bytes())
            log.info("ALERT %s [%s] %s", alert.alert_type.value, alert.severity, alert.message)

    ConsumerRunner(
        service=SERVICE,
        consumer=make_consumer(group_id=SERVICE, client_id=SERVICE),
        producer=producer,
        parsers={
            DRIVER_LOCATION_TOPIC: DriverLocation.model_validate_json,
            ORDER_EVENTS_TOPIC: OrderEvent.model_validate_json,
        },
        handler=handle,
        on_revoke=engine.reset,
    ).run(install_shutdown_handler())


if __name__ == "__main__":
    main()
