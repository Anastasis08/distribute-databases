"""Dead-letter queue: parks messages that can never be processed, with diagnostic headers."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from confluent_kafka import KafkaException, Producer

from .messages import IncomingMessage, TransientProcessingError

log = logging.getLogger(__name__)


class DeadLetterPublisher:
    def __init__(self, producer: Producer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    @classmethod
    def create(cls, bootstrap_servers: str, topic: str, client_id: str) -> DeadLetterPublisher:
        producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": f"{client_id}-dlq",
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        return cls(producer, topic)

    def publish(self, message: IncomingMessage, reason: str, detail: str) -> None:
        headers = [
            *message.headers,
            ("dlq-reason", reason.encode()),
            ("dlq-error", detail[:1000].encode("utf-8", "replace")),
            ("dlq-original-topic", message.topic.encode()),
            ("dlq-original-partition", str(message.partition).encode()),
            ("dlq-original-offset", str(message.offset).encode()),
            ("dlq-failed-at", datetime.now(UTC).isoformat().encode()),
        ]
        errors: list[str] = []
        try:
            self._producer.produce(
                self._topic,
                key=message.key,
                value=message.value,
                headers=headers,
                on_delivery=lambda err, _msg: errors.append(str(err)) if err else None,
            )
        except (KafkaException, BufferError) as exc:
            raise TransientProcessingError(f"cannot write to DLQ: {exc}") from exc
        if self._producer.flush(10) or errors:
            raise TransientProcessingError(f"DLQ delivery failed: {errors or 'timeout'}")
        log.warning("Message %s moved to DLQ '%s' (%s)", message.position, self._topic, reason)

    def close(self) -> None:
        self._producer.flush(5)
