"""Tiny geometry helpers. Distances are short (< 20 km), so simple approximations are fine."""

import math
import random
from dataclasses import dataclass

EARTH_RADIUS_M = 6_371_000
M_PER_DEG_LAT = 111_320


@dataclass(frozen=True)
class Point:
    lat: float
    lon: float


# Delivery hotspots (approximate centres).
AREAS: dict[str, Point] = {
    "Koramangala": Point(12.9352, 77.6245),
    "Indiranagar": Point(12.9784, 77.6408),
    "HSR Layout": Point(12.9116, 77.6474),
    "Whitefield": Point(12.9698, 77.7500),
    "Jayanagar": Point(12.9250, 77.5938),
    "MG Road": Point(12.9756, 77.6050),
    "Electronic City": Point(12.8452, 77.6602),
    "Marathahalli": Point(12.9591, 77.6974),
    "BTM Layout": Point(12.9166, 77.6101),
    "Malleshwaram": Point(13.0035, 77.5709),
    "Hebbal": Point(13.0358, 77.5970),
    "JP Nagar": Point(12.9063, 77.5857),
}


def haversine_m(a: Point, b: Point) -> float:
    """Great-circle distance in metres."""
    p1, p2 = math.radians(a.lat), math.radians(b.lat)
    dp, dl = p2 - p1, math.radians(b.lon - a.lon)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(h))


def bearing_deg(a: Point, b: Point) -> float:
    """Compass direction from a to b, in [0, 360)."""
    p1, p2 = math.radians(a.lat), math.radians(b.lat)
    dl = math.radians(b.lon - a.lon)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return math.degrees(math.atan2(x, y)) % 360


def offset(p: Point, north_m: float, east_m: float) -> Point:
    return Point(
        p.lat + north_m / M_PER_DEG_LAT,
        p.lon + east_m / (M_PER_DEG_LAT * math.cos(math.radians(p.lat))),
    )


def move_towards(a: Point, b: Point, distance_m: float) -> Point:
    """Move from a towards b by distance_m (stops at b)."""
    total = haversine_m(a, b)
    if total <= distance_m or total == 0:
        return b
    f = distance_m / total
    return Point(a.lat + (b.lat - a.lat) * f, a.lon + (b.lon - a.lon) * f)


def midpoint(a: Point, b: Point) -> Point:
    return Point((a.lat + b.lat) / 2, (a.lon + b.lon) / 2)


def random_point_near(center: Point, max_m: float, rng: random.Random, min_m: float = 0) -> Point:
    """Uniformly random point in a ring around center."""
    r = math.sqrt(rng.uniform(min_m**2, max_m**2))
    theta = rng.uniform(0, 2 * math.pi)
    return offset(center, r * math.cos(theta), r * math.sin(theta))
