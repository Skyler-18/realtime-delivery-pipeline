"""Central place for runtime configuration. Everything comes from env vars."""

import os

# Host-side addresses of the 3 local brokers (the EXTERNAL listeners).
# Inside Docker / on AWS this is overridden via KAFKA_BOOTSTRAP_SERVERS.
DEFAULT_BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092"


def bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)
