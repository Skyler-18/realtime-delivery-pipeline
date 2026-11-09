"""Hello-world consumer with manual offset commits.

Run one or more copies in separate terminals to watch partitions get split
across members of the same consumer group (rebalancing):

    uv run python -m delivery_pipeline.hello.consumer
    uv run python -m delivery_pipeline.hello.consumer --group another-group
"""

import argparse
import json

from confluent_kafka import Consumer, TopicPartition

from delivery_pipeline.config import bootstrap_servers

TOPIC = "hello-world"


def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    print(f"ASSIGNED partitions: {[p.partition for p in partitions]}")


def on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    print(f"REVOKED  partitions: {[p.partition for p in partitions]}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default="hello-group", help="consumer group id")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": args.group,
            # A brand-new group (no committed offsets) starts from the oldest message.
            "auto.offset.reset": "earliest",
            # We commit ourselves, only AFTER a message is processed successfully.
            # Crash before commit -> message is re-read (at-least-once, never lost).
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    print(f"Consuming '{TOPIC}' as group '{args.group}'. Ctrl+C to stop.")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"ERROR: {msg.error()}")
                continue

            payload = json.loads(msg.value())
            print(
                f"partition={msg.partition()} offset={msg.offset()} "
                f"key={msg.key().decode()} seq={payload['seq']} text={payload['text']!r}"
            )
            # Synchronous commit of offset+1 for this partition ("next message to read").
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        # close() commits nothing extra, but leaves the group cleanly -> fast rebalance.
        consumer.close()


if __name__ == "__main__":
    main()
