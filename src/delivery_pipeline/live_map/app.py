"""Live map: Kafka -> in-memory latest state -> WebSocket -> Leaflet page.

Threads vs asyncio: confluent-kafka's poll() is blocking, FastAPI is async. So the Kafka consumer
runs in its own thread and writes the latest position per driver into LiveState (lock protected).
An asyncio task reads changes once per second and pushes them to browsers. Batching like this
caps browser traffic at 1 message/second no matter how fast Kafka events arrive.

Scaling note: every live-map instance must see ALL drivers, so two instances must NOT share a
consumer group (they would split the partitions and each show half the drivers). Run one
instance, or give each instance its own group id.
"""

import asyncio
import logging
import os
import signal
import threading
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

from delivery_pipeline.config import ALERTS_TOPIC, DRIVER_LOCATION_TOPIC, LiveMapSettings
from delivery_pipeline.kafka_utils import ConsumerRunner, ReliableProducer, make_consumer
from delivery_pipeline.schemas import Alert, DriverLocation

SERVICE = "live-map"
INDEX_HTML = Path(__file__).parent / "static" / "index.html"
log = logging.getLogger("live_map")


class LiveState:
    """Latest known state, shared between the Kafka thread and the asyncio loop."""

    def __init__(self, max_alerts: int = 50) -> None:
        self._lock = threading.Lock()
        self._drivers: dict[str, dict[str, Any]] = {}
        self._changed: set[str] = set()
        self._alerts: deque[dict[str, Any]] = deque(maxlen=max_alerts)  # newest first
        self._new_alerts: list[dict[str, Any]] = []

    def apply(self, event: BaseModel) -> None:
        with self._lock:
            if isinstance(event, DriverLocation):
                self._drivers[event.driver_id] = {
                    "id": event.driver_id,
                    "lat": event.lat,
                    "lon": event.lon,
                    "speed": event.speed_kmph,
                    "status": event.status.value,
                    "order_id": event.order_id,
                    "ts": event.ts.isoformat(),
                }
                self._changed.add(event.driver_id)
            elif isinstance(event, Alert):
                alert = event.model_dump(mode="json")
                self._alerts.appendleft(alert)
                self._new_alerts.append(alert)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {"type": "snapshot", "drivers": list(self._drivers.values()), "alerts": list(self._alerts)}

    def drain_changes(self) -> dict[str, Any] | None:
        with self._lock:
            if not self._changed and not self._new_alerts:
                return None
            update = {
                "type": "update",
                "drivers": [self._drivers[d] for d in self._changed],
                "alerts": self._new_alerts,
            }
            self._changed, self._new_alerts = set(), []
            return update

    @property
    def driver_count(self) -> int:
        with self._lock:
            return len(self._drivers)


def _consume(state: LiveState, stop: threading.Event) -> None:
    try:
        ConsumerRunner(
            service=SERVICE,
            # "latest": a fresh live map doesn't care about yesterday's positions.
            consumer=make_consumer(group_id=SERVICE, client_id=SERVICE, offset_reset="latest"),
            producer=ReliableProducer(SERVICE),  # only used for dead-lettering
            parsers={
                DRIVER_LOCATION_TOPIC: DriverLocation.model_validate_json,
                ALERTS_TOPIC: Alert.model_validate_json,
            },
            handler=state.apply,
        ).run(stop)
    except Exception:
        # A web page with silently frozen data is worse than a restart: stop the whole process
        # so Docker restarts it and the consumer resumes from its last committed offset.
        log.exception("Kafka consumer crashed, shutting down")
        os.kill(os.getpid(), signal.SIGTERM)


def create_app(settings: LiveMapSettings | None = None) -> FastAPI:
    settings = settings or LiveMapSettings()
    state = LiveState()
    clients: set[WebSocket] = set()

    async def broadcast_loop() -> None:
        while True:
            await asyncio.sleep(settings.push_interval_seconds)
            update = state.drain_changes()
            if update is None:
                continue
            for ws in list(clients):
                try:
                    await ws.send_json(update)
                except Exception:  # client went away mid-send
                    clients.discard(ws)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        stop = threading.Event()
        consumer_thread = threading.Thread(target=_consume, args=(state, stop), name="kafka-consumer", daemon=True)
        consumer_thread.start()
        app.state.consumer_thread = consumer_thread
        broadcaster = asyncio.create_task(broadcast_loop())
        yield
        broadcaster.cancel()
        stop.set()
        await asyncio.to_thread(consumer_thread.join, 15)

    app = FastAPI(title="Live delivery map", lifespan=lifespan)

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(INDEX_HTML)

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "consumer_alive": app.state.consumer_thread.is_alive(),
            "drivers": state.driver_count,
            "clients": len(clients),
        }

    @app.websocket("/ws")
    async def websocket(ws: WebSocket) -> None:
        await ws.accept()
        await ws.send_json(state.snapshot())
        clients.add(ws)
        try:
            while True:
                await ws.receive_text()  # we don't expect messages; this just waits for disconnect
        except WebSocketDisconnect:
            pass
        finally:
            clients.discard(ws)

    return app
