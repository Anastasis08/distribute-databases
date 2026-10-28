"""Message contract exchanged through Kafka between the producer and the consumer."""

from .events import (
    CONTENT_TYPE,
    SCHEMA_VERSION,
    EventType,
    ProductData,
    ProductEvent,
    message_key,
)

__all__ = [
    "CONTENT_TYPE",
    "SCHEMA_VERSION",
    "EventType",
    "ProductData",
    "ProductEvent",
    "message_key",
]
