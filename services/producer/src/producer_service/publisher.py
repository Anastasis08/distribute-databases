"""Kafka publishing with delivery confirmation."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from product_contracts import ProductEvent

log = logging.getLogger(__name__)


class PublishError(RuntimeError):
    """Raised when Kafka did not acknowledge every message of a batch."""


def build_producer_config(bootstrap_servers: str, client_id: str) -> dict[str, object]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        # Durability: wait for all in-sync replicas; idempotence removes duplicates
        # caused by internal retries and keeps per-partition ordering.
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "delivery.timeout.ms": 30000,
        "linger.ms": 10,
        "compression.type": "zstd",
    }


class KafkaEventPublisher:
    def __init__(self, producer: Producer, topic: str) -> None:
        self._producer = producer
        self._topic = topic
        self._failures: list[str] = []

    @classmethod
    def create(cls, bootstrap_servers: str, topic: str, client_id: str) -> KafkaEventPublisher:
        return cls(Producer(build_producer_config(bootstrap_servers, client_id)), topic)

    def ensure_topic_exists(self, timeout: float = 10.0) -> None:
        metadata = self._producer.list_topics(topic=self._topic, timeout=timeout)
        topic = metadata.topics.get(self._topic)
        if topic is None or topic.error is not None:
            raise PublishError(f"Kafka topic '{self._topic}' is not available: {topic and topic.error}")
        log.info("Kafka topic '%s' is available (%d partitions)", self._topic, len(topic.partitions))

    def publish_batch(self, events: Iterable[ProductEvent], timeout: float) -> int:
        """Send events and block until Kafka acknowledged all of them."""
        self._failures.clear()
        sent = 0
        for event in events:
            self._produce(event)
            sent += 1
        remaining = self._producer.flush(timeout)
        if remaining:
            raise PublishError(f"{remaining} message(s) not delivered within {timeout}s")
        if self._failures:
            raise PublishError("; ".join(self._failures))
        return sent

    def _produce(self, event: ProductEvent) -> None:
        while True:
            try:
                self._producer.produce(
                    self._topic,
                    key=event.key,
                    value=event.to_bytes(),
                    headers=event.headers(),
                    on_delivery=self._make_callback(event),
                )
                break
            except BufferError:
                self._producer.poll(0.5)
            except KafkaException as exc:
                raise PublishError(str(exc)) from exc
        self._producer.poll(0)

    def _make_callback(self, event: ProductEvent):
        def on_delivery(error: KafkaError | None, message: Message) -> None:
            if error is not None:
                self._failures.append(f"id={event.data.id}: {error}")
                log.error("Delivery failed id=%s: %s", event.data.id, error)
                return
            log.info(
                "Published %s id=%s sku=%s -> %s[partition=%s offset=%s]",
                event.event_type,
                event.data.id,
                event.data.sku,
                message.topic(),
                message.partition(),
                message.offset(),
            )

        return on_delivery

    def close(self, timeout: float = 10.0) -> None:
        remaining = self._producer.flush(timeout)
        if remaining:
            log.warning("%d message(s) were still in the queue on shutdown", remaining)
