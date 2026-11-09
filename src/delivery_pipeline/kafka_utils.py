"""Reusable Kafka building blocks: a reliable producer, a consumer factory, a dead-letter
publisher, and a consume loop with at-least-once semantics."""

import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from pydantic import BaseModel, ValidationError

from delivery_pipeline.config import DEAD_LETTER_TOPIC, bootstrap_servers

log = logging.getLogger(__name__)

Parser = Callable[[bytes], BaseModel]
Handler = Callable[[BaseModel], None]


class ReliableProducer:
    """Idempotent producer that remembers failed deliveries, so callers can refuse to commit."""

    def __init__(self, client_id: str) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers(),
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
                "compression.type": "lz4",
            }
        )
        self.failed_deliveries = 0

    def produce(
        self,
        topic: str,
        key: str | bytes | None,
        value: bytes | None,
        headers: list[tuple[str, str]] | None = None,
    ) -> None:
        self._producer.produce(topic, key=key, value=value, headers=headers, on_delivery=self._on_delivery)
        self._producer.poll(0)  # serve delivery callbacks

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed_deliveries += 1
            log.error("Delivery to %s failed: %s", msg.topic(), err)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def flush_or_raise(self, timeout: float = 10.0) -> None:
        """Block until everything is acknowledged. Raise if anything was not."""
        remaining = self._producer.flush(timeout)
        if remaining or self.failed_deliveries:
            raise RuntimeError(
                f"{remaining} undelivered / {self.failed_deliveries} failed messages; refusing to commit offsets"
            )


def make_consumer(group_id: str, client_id: str, offset_reset: str = "earliest") -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": group_id,
            "client.id": client_id,
            "auto.offset.reset": offset_reset,
            # We decide what gets committed, and when:
            "enable.auto.commit": False,  # no background commits
            "enable.auto.offset.store": False,  # poll() alone doesn't mark a message as done
        }
    )


class DeadLetterPublisher:
    """Parks a message we cannot process in the dead-letter topic, unchanged, with the reason
    and its origin in headers, so it can be inspected and replayed later."""

    def __init__(self, producer: ReliableProducer, service: str) -> None:
        self._producer = producer
        self._service = service

    def publish(self, msg: Message, error: str) -> None:
        headers = [
            ("dlq.error", error[:1000]),
            ("dlq.service", self._service),
            ("dlq.source.topic", msg.topic()),
            ("dlq.source.partition", str(msg.partition())),
            ("dlq.source.offset", str(msg.offset())),
            ("dlq.failed_at", datetime.now(UTC).isoformat()),
        ]
        self._producer.produce(DEAD_LETTER_TOPIC, key=msg.key(), value=msg.value(), headers=headers)
        log.warning(
            "Sent to dead-letter: %s[%d]@%d: %s", msg.topic(), msg.partition(), msg.offset(), error
        )


class InvalidMessage(Exception):
    """The message itself is bad (retrying will never fix it) -> dead-letter topic."""


def _summarize(exc: ValidationError) -> str:
    return "; ".join(
        f"{'.'.join(str(p) for p in e['loc']) or '<root>'}: {e['msg']}"
        for e in exc.errors(include_url=False)[:3]
    )


class ConsumerRunner:
    """poll -> validate -> handle -> store offset; commit every `commit_interval_s`.

    Delivery guarantee: AT-LEAST-ONCE. An offset is committed only after
      1. its message was handled (or parked in the dead-letter topic), and
      2. every message we produced while handling it was acknowledged by Kafka.
    If we crash in between, the uncommitted messages are simply processed again.

    Error policy:
      - Bad message (invalid JSON / schema)  -> dead-letter topic, keep going.
      - Bug or infrastructure error           -> exception propagates, process exits, Docker restarts it,
                                                 and it resumes from the last committed offset.
    """

    def __init__(
        self,
        *,
        service: str,
        consumer: Consumer,
        producer: ReliableProducer,
        parsers: dict[str, Parser],
        handler: Handler,
        on_revoke: Callable[[], None] | None = None,
        commit_interval_s: float = 1.0,
        stats_interval_s: float = 30.0,
    ) -> None:
        self._service = service
        self._consumer = consumer
        self._producer = producer
        self._dlq = DeadLetterPublisher(producer, service)
        self._parsers = parsers
        self._handler = handler
        self._on_revoke_cb = on_revoke
        self._commit_interval_s = commit_interval_s
        self._stats_interval_s = stats_interval_s
        self.processed = 0
        self.dead_lettered = 0

    def run(self, stop: threading.Event) -> None:
        self._consumer.subscribe(list(self._parsers), on_assign=self._on_assign, on_revoke=self._on_revoke)
        log.info("Consuming %s", list(self._parsers))
        last_commit = last_stats = time.monotonic()
        try:
            while not stop.is_set():
                msg = self._consumer.poll(0.5)
                if msg is not None:
                    self._process(msg)
                now = time.monotonic()
                if now - last_commit >= self._commit_interval_s:
                    self._commit()
                    last_commit = now
                if now - last_stats >= self._stats_interval_s:
                    log.info("stats processed=%d dead_lettered=%d", self.processed, self.dead_lettered)
                    last_stats = now
        finally:
            try:
                self._commit()
            finally:
                self._consumer.close()  # leave the group cleanly -> fast rebalance
                log.info("Consumer closed")

    def _process(self, msg: Message) -> None:
        if msg.error():
            log.error("Consumer error: %s", msg.error())
            return
        try:
            event = self._parse(msg)
        except InvalidMessage as exc:
            self._dlq.publish(msg, str(exc))
            self.dead_lettered += 1
        else:
            self._handler(event)
            self.processed += 1
        # Mark as done. It is actually committed by the next _commit().
        self._consumer.store_offsets(message=msg)

    def _parse(self, msg: Message) -> BaseModel:
        value = msg.value()
        if not value:
            raise InvalidMessage("empty message value")
        try:
            return self._parsers[msg.topic()](value)
        except ValidationError as exc:  # pydantic also raises this for invalid JSON
            raise InvalidMessage(_summarize(exc)) from exc

    def _commit(self) -> None:
        self._producer.flush_or_raise()  # never commit input before our output is safely in Kafka
        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError._NO_OFFSET:  # nothing new to commit is fine
                raise

    def _on_assign(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("Assigned %s", [f"{p.topic}[{p.partition}]" for p in partitions])

    def _on_revoke(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("Revoked %s", [f"{p.topic}[{p.partition}]" for p in partitions])
        # Commit what we finished so the next owner of these partitions starts right after it.
        self._commit()
        if self._on_revoke_cb:
            self._on_revoke_cb()
