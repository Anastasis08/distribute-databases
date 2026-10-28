"""Versioned JSON contract of the `products` Kafka topic.

Message layout:
    key   = product id as UTF-8 text (e.g. b"101")
    value = ProductEvent serialized to JSON (camelCase field names)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel

SCHEMA_VERSION: Literal[1] = 1
CONTENT_TYPE = "application/json"
SOURCE = "postgres.public.products"

EventType = Literal["product.created", "product.updated"]

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)


class _ContractModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        validate_by_name=True,
        validate_by_alias=True,
        serialize_by_alias=True,
        frozen=True,
        extra="ignore",
    )


class ProductData(_ContractModel):
    """Snapshot of one `products` row at the moment it was read by the producer."""

    id: int = Field(gt=0, description="Stable identifier: SQL primary key, Kafka key, MongoDB _id.")
    sku: str = Field(min_length=1, max_length=32)
    name: str = Field(min_length=1, max_length=200)
    category: str = Field(min_length=1, max_length=100)
    price: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    quantity: int = Field(ge=0)
    created_at: AwareDatetime
    updated_at: AwareDatetime

    @field_validator("created_at", "updated_at")
    @classmethod
    def _normalize_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @property
    def version(self) -> int:
        """Monotonic row version: `updated_at` as microseconds since the Unix epoch."""
        return (self.updated_at - _EPOCH) // _MICROSECOND

    @property
    def is_new(self) -> bool:
        """True while the row has never been modified after INSERT."""
        return self.created_at == self.updated_at


class ProductEvent(_ContractModel):
    """Envelope with metadata around the product snapshot."""

    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    schema_version: Literal[1] = SCHEMA_VERSION
    occurred_at: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    source: str = SOURCE
    data: ProductData

    @classmethod
    def from_product(cls, product: ProductData) -> ProductEvent:
        event_type: EventType = "product.created" if product.is_new else "product.updated"
        return cls(event_type=event_type, data=product)

    @property
    def key(self) -> bytes:
        return message_key(self.data.id)

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, payload: bytes | str) -> ProductEvent:
        return cls.model_validate_json(payload)

    def headers(self) -> list[tuple[str, bytes]]:
        return [
            ("content-type", CONTENT_TYPE.encode()),
            ("event-type", self.event_type.encode()),
            ("event-id", str(self.event_id).encode()),
            ("schema-version", str(self.schema_version).encode()),
        ]


def message_key(product_id: int) -> bytes:
    """Kafka key of a product: guarantees per-product ordering inside one partition."""
    return str(product_id).encode("ascii")
