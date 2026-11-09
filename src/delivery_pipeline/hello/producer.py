"""Hello-world producer.

Sends 12 JSON messages keyed by 3 users. Watch the output: every message with
the same key lands on the same partition -> per-key ordering is guaranteed.

Run: uv run python -m delivery_pipeline.hello.producer
"""

import json
import sys
import time

from confluent_kafka import KafkaError, Message, Producer

from delivery_pipeline.config import bootstrap_servers

TOPIC = "hello-world"
USERS = ["user-1", "user-2", "user-3"]


def on_delivery(err: KafkaError | None, msg: Message) -> None:
    """Called once per message when the broker confirms (or rejects) the write."""
    key = msg.key().decode() if msg.key() else None
    if err is not None:
        print(f"FAILED   key={key}: {err}")
    else:
        print(f"OK       key={key} -> partition={msg.partition()} offset={msg.offset()}")


def main() -> None:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "client.id": "hello-producer",
            # Wait until all in-sync replicas (>= min.insync.replicas=2) have the write.
            "acks": "all",
            # Broker de-duplicates retries using producer id + sequence number,
            # so a network retry can never create a duplicate or reorder messages.
            "enable.idempotence": True,
            # Small batching window: better throughput for a tiny latency cost.
            "linger.ms": 5,
        }
    )

    for i in range(12):
        key = USERS[i % len(USERS)]
        value = {"seq": i, "user": key, "text": f"hello #{i}", "ts": time.time()}
        # produce() is async: it only puts the message in a local buffer.
        producer.produce(TOPIC, key=key, value=json.dumps(value).encode(), on_delivery=on_delivery)
        # poll(0) gives librdkafka a chance to fire delivery callbacks.
        producer.poll(0)

    # Block until every buffered message is delivered (or 10s pass).
    remaining = producer.flush(10)
    if remaining:
        print(f"{remaining} message(s) were NOT delivered")
        sys.exit(1)
    print("All messages delivered.")


if __name__ == "__main__":
    main()
