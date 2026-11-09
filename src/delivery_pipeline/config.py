"""Runtime configuration. Everything comes from environment variables (12-factor style).

pydantic-settings reads env vars into typed fields, e.g. SIM_NUM_DRIVERS=50 -> SimulatorSettings.num_drivers.
Bad values (SIM_NUM_DRIVERS=abc) fail fast at startup instead of deep inside the code.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Topic names are part of the contract between services, so they live in one place.
DRIVER_LOCATION_TOPIC = "driver-location"
ORDER_EVENTS_TOPIC = "order-events"
ALERTS_TOPIC = "alerts"
DEAD_LETTER_TOPIC = "dead-letter"

# Host-side addresses of the 3 local brokers (the EXTERNAL listeners).
# Inside Docker / on AWS this is overridden via KAFKA_BOOTSTRAP_SERVERS.
DEFAULT_BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_")

    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS


class SimulatorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIM_")

    num_drivers: int = Field(100, ge=1, le=1000)
    tick_seconds: float = Field(2.0, gt=0)
    # Little's law: busy drivers ~= orders/min * minutes per delivery (~12) -> 6/min keeps ~70 of 100 busy.
    orders_per_minute: float = Field(6.0, ge=0)
    malformed_rate: float = Field(0.01, ge=0, le=1)  # fraction of location events followed by a bad one
    cancel_prob_per_min: float = 0.01  # per order not yet picked up
    max_unassigned_seconds: float = 300  # no free driver for this long -> order cancelled
    stuck_prob_per_min: float = 0.02  # per busy driver: gets stuck for 4-8 minutes
    speeding_prob_per_min: float = 0.03  # per moving driver: drives at 65-95 km/h for a few ticks
    seed: int | None = None  # set for reproducible runs


class AlertSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ALERT_")

    speed_limit_kmph: float = 60
    speeding_cooldown_seconds: float = 300  # max one speeding alert per driver per 5 min
    stationary_seconds: float = 180
    stationary_radius_m: float = 30
    delivery_sla_minutes: float = 20


class LiveMapSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LIVEMAP_")

    host: str = "0.0.0.0"
    port: int = 8000
    push_interval_seconds: float = 1.0


def bootstrap_servers() -> str:
    return KafkaSettings().bootstrap_servers
