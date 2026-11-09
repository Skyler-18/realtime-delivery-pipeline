"""Pure simulation of drivers and orders. No Kafka in here, so it is easy to unit-test.

Each call to World.step(now) advances the world by one tick and returns the events that happened.
Order lifecycle: placed -> assigned -> (driver drives to restaurant, waits for food) -> picked_up
-> (drives to customer) -> delivered. Orders not yet picked up can be cancelled.
"""

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from delivery_pipeline.config import SimulatorSettings
from delivery_pipeline.geo import (
    AREAS,
    Point,
    bearing_deg,
    haversine_m,
    midpoint,
    move_towards,
    random_point_near,
)
from delivery_pipeline.schemas import DriverLocation, DriverStatus, OrderEvent, OrderStatus

MAX_ASSIGN_DISTANCE_M = 6_000
IDLE_REPOSITION_PROB = 0.01  # per tick: an idle driver moves to a nearby spot


@dataclass
class Order:
    order_id: str
    area: str
    restaurant: Point
    customer: Point
    amount_inr: float
    placed_at: datetime
    status: OrderStatus = OrderStatus.PLACED
    driver_id: str | None = None


@dataclass
class Driver:
    driver_id: str
    pos: Point
    cruise_kmph: float
    status: DriverStatus = DriverStatus.IDLE
    order_id: str | None = None
    route: list[Point] = field(default_factory=list)  # remaining waypoints
    speed_kmph: float = 0.0
    heading_deg: float = 0.0
    wait_until: datetime | None = None  # waiting for food at the restaurant
    stuck_until: datetime | None = None  # anomaly: not moving (traffic jam, breakdown...)
    speeding_ticks: int = 0  # anomaly: remaining ticks of speeding


class World:
    def __init__(self, settings: SimulatorSettings, rng: random.Random) -> None:
        self.s = settings
        self.rng = rng
        self.orders: dict[str, Order] = {}  # open orders only
        self.drivers = [self._new_driver(i) for i in range(1, settings.num_drivers + 1)]

    # ---------------------------------------------------------------- public API

    def step(self, now: datetime) -> tuple[list[DriverLocation], list[OrderEvent]]:
        order_events: list[OrderEvent] = []
        order_events += self._spawn_orders(now)
        order_events += self._cancel_orders(now)
        order_events += self._assign_orders(now)
        for driver in self.drivers:
            order_events += self._move_driver(driver, now)
        locations = [self._location_event(d, now) for d in self.drivers]
        return locations, order_events

    def stats(self) -> dict[str, int]:
        counts = {s.value: 0 for s in DriverStatus}
        for d in self.drivers:
            counts[d.status.value] += 1
        counts["open_orders"] = len(self.orders)
        counts["stuck"] = sum(1 for d in self.drivers if d.stuck_until)
        return counts

    # ---------------------------------------------------------------- orders

    def _chance(self, prob_per_min: float) -> bool:
        return self.rng.random() < prob_per_min * self.s.tick_seconds / 60

    def _spawn_orders(self, now: datetime) -> list[OrderEvent]:
        expected = self.s.orders_per_minute * self.s.tick_seconds / 60
        count = int(expected) + (1 if self.rng.random() < expected - int(expected) else 0)
        events = []
        for _ in range(count):
            area = self.rng.choice(list(AREAS))
            restaurant = random_point_near(AREAS[area], 1_500, self.rng)
            order = Order(
                order_id=f"O{uuid.uuid4().hex[:12]}",
                area=area,
                restaurant=restaurant,
                customer=random_point_near(restaurant, 4_000, self.rng, min_m=500),
                amount_inr=round(self.rng.uniform(150, 900), 2),
                placed_at=now,
            )
            self.orders[order.order_id] = order
            events.append(self._order_event(order, now))
        return events

    def _cancel_orders(self, now: datetime) -> list[OrderEvent]:
        events = []
        for order in list(self.orders.values()):
            if order.status not in (OrderStatus.PLACED, OrderStatus.ASSIGNED):
                continue  # food already picked up: too late to cancel
            if order.status == OrderStatus.PLACED and (now - order.placed_at).total_seconds() > self.s.max_unassigned_seconds:
                reason = "no_driver_available"
            elif self._chance(self.s.cancel_prob_per_min):
                reason = "customer_cancelled"
            else:
                continue
            order.status = OrderStatus.CANCELLED
            if order.driver_id:
                self._free_driver(self._driver(order.driver_id))
            del self.orders[order.order_id]
            events.append(self._order_event(order, now, cancel_reason=reason))
        return events

    def _assign_orders(self, now: datetime) -> list[OrderEvent]:
        idle = [d for d in self.drivers if d.status == DriverStatus.IDLE and d.stuck_until is None]
        events = []
        waiting = sorted(
            (o for o in self.orders.values() if o.status == OrderStatus.PLACED), key=lambda o: o.placed_at
        )
        for order in waiting:
            if not idle:
                break
            nearest = min(idle, key=lambda d: haversine_m(d.pos, order.restaurant))
            if haversine_m(nearest.pos, order.restaurant) > MAX_ASSIGN_DISTANCE_M:
                continue
            idle.remove(nearest)
            order.status, order.driver_id = OrderStatus.ASSIGNED, nearest.driver_id
            nearest.status, nearest.order_id = DriverStatus.TO_PICKUP, order.order_id
            nearest.route = self._route(nearest.pos, order.restaurant)
            events.append(self._order_event(order, now))
        return events

    # ---------------------------------------------------------------- drivers

    def _new_driver(self, i: int) -> Driver:
        home = AREAS[self.rng.choice(list(AREAS))]
        return Driver(
            driver_id=f"D{i:04d}",
            pos=random_point_near(home, 2_000, self.rng),
            cruise_kmph=self.rng.uniform(18, 32),
        )

    def _driver(self, driver_id: str) -> Driver:
        return next(d for d in self.drivers if d.driver_id == driver_id)

    @staticmethod
    def _free_driver(driver: Driver) -> None:
        driver.status, driver.order_id = DriverStatus.IDLE, None
        driver.route, driver.wait_until = [], None

    def _route(self, start: Point, end: Point) -> list[Point]:
        """Straight line with one random bend, so paths look less artificial.
        (Real road routing would need a routing engine like OSRM.)"""
        dist = haversine_m(start, end)
        if dist < 300:
            return [end]
        return [random_point_near(midpoint(start, end), 0.2 * dist, self.rng), end]

    def _move_driver(self, d: Driver, now: datetime) -> list[OrderEvent]:
        events: list[OrderEvent] = []

        # Anomaly: stuck (only busy drivers on the road; that's what the alert rule watches).
        if d.stuck_until and now >= d.stuck_until:
            d.stuck_until = None
        if d.status != DriverStatus.IDLE and not d.stuck_until and not d.wait_until and self._chance(self.s.stuck_prob_per_min):
            d.stuck_until = now + timedelta(seconds=self.rng.uniform(240, 480))
        if d.stuck_until:
            d.speed_kmph = 0.0
            return events

        # Waiting at the restaurant for the food.
        if d.wait_until:
            if now < d.wait_until:
                d.speed_kmph = 0.0
                return events
            order = self.orders[d.order_id]
            order.status = OrderStatus.PICKED_UP
            d.status, d.wait_until = DriverStatus.TO_DROPOFF, None
            d.route = self._route(d.pos, order.customer)
            events.append(self._order_event(order, now))

        if not d.route:
            d.speed_kmph = 0.0
            if d.status == DriverStatus.IDLE and self.rng.random() < IDLE_REPOSITION_PROB:
                d.route = self._route(d.pos, random_point_near(d.pos, 2_000, self.rng))
            return events

        # Driving.
        if d.speeding_ticks == 0 and self._chance(self.s.speeding_prob_per_min):
            d.speeding_ticks = self.rng.randint(3, 6)
        if d.speeding_ticks:
            d.speeding_ticks -= 1
            d.speed_kmph = round(self.rng.uniform(65, 95), 1)
        else:
            d.speed_kmph = round(max(5.0, self.rng.gauss(d.cruise_kmph, 3)), 1)

        previous = d.pos
        self._advance(d, d.speed_kmph / 3.6 * self.s.tick_seconds)
        if d.pos != previous:
            d.heading_deg = bearing_deg(previous, d.pos)

        if not d.route:  # arrived
            if d.status == DriverStatus.TO_PICKUP:
                d.wait_until = now + timedelta(seconds=self.rng.uniform(30, 120))
            elif d.status == DriverStatus.TO_DROPOFF:
                order = self.orders.pop(d.order_id)
                order.status = OrderStatus.DELIVERED
                events.append(self._order_event(order, now))
                self._free_driver(d)
        return events

    @staticmethod
    def _advance(d: Driver, distance_m: float) -> None:
        while distance_m > 0 and d.route:
            target = d.route[0]
            step = haversine_m(d.pos, target)
            if step <= distance_m:
                d.pos = target
                d.route.pop(0)
                distance_m -= step
            else:
                d.pos = move_towards(d.pos, target, distance_m)
                distance_m = 0

    # ---------------------------------------------------------------- events

    @staticmethod
    def _order_event(o: Order, now: datetime, cancel_reason: str | None = None) -> OrderEvent:
        return OrderEvent(
            event_id=uuid.uuid4().hex,
            ts=now,
            order_id=o.order_id,
            status=o.status,
            area=o.area,
            placed_at=o.placed_at,
            restaurant_lat=round(o.restaurant.lat, 6),
            restaurant_lon=round(o.restaurant.lon, 6),
            customer_lat=round(o.customer.lat, 6),
            customer_lon=round(o.customer.lon, 6),
            amount_inr=o.amount_inr,
            driver_id=o.driver_id,
            cancel_reason=cancel_reason,
        )

    @staticmethod
    def _location_event(d: Driver, now: datetime) -> DriverLocation:
        return DriverLocation(
            event_id=uuid.uuid4().hex,
            ts=now,
            driver_id=d.driver_id,
            lat=round(d.pos.lat, 6),
            lon=round(d.pos.lon, 6),
            speed_kmph=d.speed_kmph,
            heading_deg=round(d.heading_deg, 1) % 360,
            status=d.status,
            order_id=d.order_id,
        )
