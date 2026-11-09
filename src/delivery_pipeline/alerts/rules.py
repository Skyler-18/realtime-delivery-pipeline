"""Stateful alert rules. Pure logic (events in, alerts out), no Kafka, easy to unit-test.

Time: rules use EVENT time (the `ts` inside events), not the wall clock. The engine's clock is
the highest event timestamp seen so far. So replaying old data from Kafka produces exactly the
same alerts, and a consumer that is lagging behind doesn't raise false "too slow" alerts.

State is keyed by driver_id / order_id, the same keys the topics are partitioned by. So when
several alert instances share the work, each one owns complete state for its own keys.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from delivery_pipeline.config import AlertSettings
from delivery_pipeline.geo import Point, haversine_m
from delivery_pipeline.schemas import (
    Alert,
    AlertType,
    DriverLocation,
    DriverStatus,
    Event,
    OrderEvent,
    OrderStatus,
)

_ALERT_NAMESPACE = uuid.UUID("6f1c2a52-6a8e-4c43-9d0e-4f7b2b8f3c11")
SLA_CHECK_EVERY = timedelta(seconds=1)


def _alert_id(*parts: str) -> str:
    """Deterministic id: re-processing the same input (at-least-once!) yields the same alert id,
    so downstream consumers can de-duplicate."""
    return uuid.uuid5(_ALERT_NAMESPACE, ":".join(parts)).hex


@dataclass
class _DriverState:
    last_pos: Point
    anchor: Point  # where the driver was when they stopped moving
    anchor_ts: datetime
    stationary_alerted: bool = False
    last_speeding_alert: datetime | None = None


@dataclass
class _OpenOrder:
    placed_at: datetime
    restaurant: Point
    driver_id: str | None = None
    delayed_alerted: bool = False


class AlertEngine:
    def __init__(self, settings: AlertSettings) -> None:
        self.s = settings
        self._drivers: dict[str, _DriverState] = {}
        self._orders: dict[str, _OpenOrder] = {}
        self._clock: datetime | None = None
        self._last_sla_check: datetime | None = None

    def process(self, event: Event) -> list[Alert]:
        if self._clock is None or event.ts > self._clock:
            self._clock = event.ts
        if isinstance(event, DriverLocation):
            alerts = self._on_location(event)
        elif isinstance(event, OrderEvent):
            alerts = self._on_order(event)
        else:
            alerts = []
        return alerts + self._check_delivery_sla()

    def reset(self) -> None:
        """Called on partition revoke: state for partitions we may no longer own is dropped."""
        self._drivers.clear()
        self._orders.clear()

    # ---------------------------------------------------------------- driver rules

    def _on_location(self, ev: DriverLocation) -> list[Alert]:
        pos = Point(ev.lat, ev.lon)
        st = self._drivers.get(ev.driver_id)
        if st is None:
            st = self._drivers[ev.driver_id] = _DriverState(last_pos=pos, anchor=pos, anchor_ts=ev.ts)
        st.last_pos = pos
        return [a for a in (self._speeding(ev, st), self._stationary(ev, st, pos)) if a]

    def _speeding(self, ev: DriverLocation, st: _DriverState) -> Alert | None:
        if ev.speed_kmph <= self.s.speed_limit_kmph:
            return None
        cooldown = timedelta(seconds=self.s.speeding_cooldown_seconds)
        if st.last_speeding_alert and ev.ts - st.last_speeding_alert < cooldown:
            return None
        st.last_speeding_alert = ev.ts
        return Alert(
            event_id=_alert_id(AlertType.SPEEDING, ev.event_id),
            ts=ev.ts,
            alert_type=AlertType.SPEEDING,
            severity="high" if ev.speed_kmph > self.s.speed_limit_kmph + 25 else "medium",
            message=f"{ev.driver_id} at {ev.speed_kmph:.0f} km/h (limit {self.s.speed_limit_kmph:.0f})",
            driver_id=ev.driver_id,
            order_id=ev.order_id,
            lat=ev.lat,
            lon=ev.lon,
        )

    def _stationary(self, ev: DriverLocation, st: _DriverState, pos: Point) -> Alert | None:
        moved = haversine_m(st.anchor, pos) > self.s.stationary_radius_m
        if ev.status == DriverStatus.IDLE or moved:
            # Idle drivers are allowed to wait; a moving driver starts a new "episode".
            st.anchor, st.anchor_ts, st.stationary_alerted = pos, ev.ts, False
            return None
        still_for = (ev.ts - st.anchor_ts).total_seconds()
        if st.stationary_alerted or still_for < self.s.stationary_seconds:
            return None
        st.stationary_alerted = True
        return Alert(
            event_id=_alert_id(AlertType.STATIONARY, ev.driver_id, st.anchor_ts.isoformat()),
            ts=ev.ts,
            alert_type=AlertType.STATIONARY,
            severity="medium",
            message=f"{ev.driver_id} stationary for {still_for / 60:.0f} min while {ev.status.value}",
            driver_id=ev.driver_id,
            order_id=ev.order_id,
            lat=ev.lat,
            lon=ev.lon,
        )

    # ---------------------------------------------------------------- order rules

    def _on_order(self, ev: OrderEvent) -> list[Alert]:
        if ev.status in (OrderStatus.DELIVERED, OrderStatus.CANCELLED):
            self._orders.pop(ev.order_id, None)
            return []
        order = self._orders.get(ev.order_id)
        if order is None:
            # placed_at is on every event, so we can start tracking an order from any of its events.
            order = self._orders[ev.order_id] = _OpenOrder(
                placed_at=ev.placed_at, restaurant=Point(ev.restaurant_lat, ev.restaurant_lon)
            )
        order.driver_id = ev.driver_id or order.driver_id
        return []

    def _check_delivery_sla(self) -> list[Alert]:
        """Timer-style rule: fires on the *absence* of a 'delivered' event."""
        if self._clock is None:
            return []
        if self._last_sla_check and self._clock - self._last_sla_check < SLA_CHECK_EVERY:
            return []
        self._last_sla_check = self._clock
        sla = timedelta(minutes=self.s.delivery_sla_minutes)
        alerts = []
        for order_id, order in self._orders.items():
            age = self._clock - order.placed_at
            if order.delayed_alerted or age <= sla:
                continue
            order.delayed_alerted = True
            driver = self._drivers.get(order.driver_id) if order.driver_id else None
            pos = driver.last_pos if driver else order.restaurant
            alerts.append(
                Alert(
                    event_id=_alert_id(AlertType.DELIVERY_DELAYED, order_id),
                    ts=self._clock,
                    alert_type=AlertType.DELIVERY_DELAYED,
                    severity="high",
                    message=f"Order {order_id} open for {age.total_seconds() / 60:.0f} min (SLA {self.s.delivery_sla_minutes:.0f})",
                    driver_id=order.driver_id,
                    order_id=order_id,
                    lat=pos.lat,
                    lon=pos.lon,
                )
            )
        return alerts
