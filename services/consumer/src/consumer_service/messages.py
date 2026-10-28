"""Transport-independent view of a Kafka record and processing errors."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


class KafkaRecord(Protocol):
    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def headers(self) -> list[tuple[str, bytes]] | None: ...


@dataclass(frozen=True, slots=True)
class IncomingMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes]] = field(default_factory=list)

    @classmethod
    def from_kafka(cls, record: KafkaRecord) -> IncomingMessage:
        return cls(
            topic=record.topic(),
            partition=record.partition(),
            offset=record.offset(),
            key=record.key(),
            value=record.value(),
            headers=list(record.headers() or []),
        )

    @property
    def position(self) -> str:
        return f"{self.topic}[{self.partition}]@{self.offset}"


class PermanentMessageError(Exception):
    """The message can never be processed (bad payload, rejected document): send it to the DLQ."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


class TransientProcessingError(Exception):
    """A dependency is temporarily unavailable: retry the same message later."""
