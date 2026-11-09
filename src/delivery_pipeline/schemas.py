"""Event contracts shared by producers and consumers.

Rules we follow:
- Every event carries `schema_version`. Unknown versions are rejected (-> dead-letter topic).
- Unknown extra fields are IGNORED ("tolerant reader"): a producer may add an optional field
  without breaking older consumers. Removing/renaming a field is a breaking change -> new version.
- Timestamps must be timezone-aware (UTC). Naive datetimes are a classic source of bugs.
"""

from enum import StrEnum
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1

# Generous bounding box around Bengaluru; coordinates outside it are treated as bad data.
LAT_MIN, LAT_MAX = 12.7, 13.3
LON_MIN, LON_MAX = 77.3, 77.9

Lat = Field(ge=LAT_MIN, le=LAT_MAX)
Lon = Field(ge=LON_MIN, le=LON_MAX)


class DriverStatus(StrEnum):
    IDLE = "idle"
    TO_PICKUP = "to_pickup"
    TO_DROPOFF = "to_dropoff"


class OrderStatus(StrEnum):
    PLACED = "placed"
    ASSIGNED = "assigned"
    PICKED_UP = "picked_up"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class AlertType(StrEnum):
    SPEEDING = "speeding"
    STATIONARY = "stationary"
    DELIVERY_DELAYED = "delivery_delayed"


class Event(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore")

    schema_version: Literal[1] = SCHEMA_VERSION
    event_id: str = Field(min_length=1)
    ts: AwareDatetime

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()


class DriverLocation(Event):
    driver_id: str = Field(min_length=1)
    lat: float = Lat
    lon: float = Lon
    speed_kmph: float = Field(ge=0, le=250)
    heading_deg: float = Field(ge=0, lt=360)
    status: DriverStatus
    order_id: str | None = None


class OrderEvent(Event):
    """One event per lifecycle step. Order details are repeated on every event (denormalized)
    so any consumer can make sense of a single event without joining against other data."""

    order_id: str = Field(min_length=1)
    status: OrderStatus
    area: str
    placed_at: AwareDatetime
    restaurant_lat: float = Lat
    restaurant_lon: float = Lon
    customer_lat: float = Lat
    customer_lon: float = Lon
    amount_inr: float = Field(gt=0)
    driver_id: str | None = None
    cancel_reason: str | None = None


class Alert(Event):
    alert_type: AlertType
    severity: Literal["low", "medium", "high"]
    message: str
    driver_id: str | None = None
    order_id: str | None = None
    lat: float | None = None
    lon: float | None = None
