"""Kafka poll loop with at-least-once semantics.

Offsets are committed manually and only after the message was stored in MongoDB
(or parked in the DLQ). On a transient failure the consumer seeks back to the same
offset and retries with exponential backoff, so no message is lost or skipped.
"""

from __future__ import annotations

import logging
import threading
from typing import Protocol

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from .handler import ProductMessageHandler
from .messages import IncomingMessage, PermanentMessageError, TransientProcessingError

log = logging.getLogger(__name__)


class DeadLetterSink(Protocol):
    def publish(self, message: IncomingMessage, reason: str, detail: str) -> None: ...


def build_consumer_config(bootstrap_servers: str, group_id: str, client_id: str) -> dict[str, object]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "client.id": client_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 300000,
    }


class ConsumerRunner:
    def __init__(
        self,
        consumer: Consumer,
        handler: ProductMessageHandler,
        dead_letters: DeadLetterSink,
        topic: str,
        *,
        retry_initial: float,
        retry_max: float,
    ) -> None:
        self._consumer = consumer
        self._handler = handler
        self._dead_letters = dead_letters
        self._topic = topic
        self._retry_initial = retry_initial
        self._retry_max = retry_max
        self._attempt = 0

    def run(self, stop: threading.Event) -> None:
        self._consumer.subscribe([self._topic], on_assign=self._on_assign, on_revoke=self._on_revoke)
        log.info("Subscribed to topic '%s'", self._topic)
        while not stop.is_set():
            record = self._consumer.poll(1.0)
            if record is None:
                continue
            if record.error() is not None:
                self._on_kafka_error(record.error())
                continue
            self.process(IncomingMessage.from_kafka(record), record, stop)

    def process(self, message: IncomingMessage, record, stop: threading.Event) -> None:
        try:
            self._handle(message)
        except TransientProcessingError as exc:
            self._attempt += 1
            delay = min(self._retry_initial * 2 ** (self._attempt - 1), self._retry_max)
            log.error(
                "Transient failure on %s (attempt %d): %s; retrying in %.1fs",
                message.position,
                self._attempt,
                exc,
                delay,
            )
            self._consumer.seek(TopicPartition(message.topic, message.partition, message.offset))
            stop.wait(delay)
            return
        self._attempt = 0
        self._commit(record, message)

    def _handle(self, message: IncomingMessage) -> None:
        log.debug("Received %s key=%r", message.position, message.key)
        try:
            event, result = self._handler.handle(message)
        except PermanentMessageError as exc:
            log.error("Cannot process %s: %s", message.position, exc)
            self._dead_letters.publish(message, exc.reason, exc.detail)
            return
        product = event.data
        log.info(
            "Consumed %s from %s: id=%s sku=%s price=%s qty=%s -> MongoDB %s",
            event.event_type,
            message.position,
            product.id,
            product.sku,
            product.price,
            product.quantity,
            result.value,
        )

    def _commit(self, record, message: IncomingMessage) -> None:
        try:
            self._consumer.commit(message=record, asynchronous=False)
        except KafkaException as exc:
            log.warning(
                "Offset commit for %s failed (%s); message may be redelivered, "
                "which is safe thanks to idempotent upserts",
                message.position,
                exc,
            )

    def _on_kafka_error(self, error: KafkaError) -> None:
        if error.fatal():
            raise KafkaException(error)
        log.warning("Kafka error: %s", error)

    @staticmethod
    def _on_assign(_consumer, partitions: list[TopicPartition]) -> None:
        log.info("Partitions assigned: %s", [p.partition for p in partitions])

    @staticmethod
    def _on_revoke(_consumer, partitions: list[TopicPartition]) -> None:
        log.info("Partitions revoked: %s", [p.partition for p in partitions])
