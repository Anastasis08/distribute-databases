from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from consumer_service.messages import IncomingMessage
from product_contracts import ProductData, ProductEvent

BASE = datetime(2026, 10, 13, 12, 0, 0, 123456, tzinfo=UTC)


def make_event(product_id=101, *, seconds=0, micros=0, price="1200.00", name="Keyboard"):
    updated = BASE + timedelta(seconds=seconds, microseconds=micros)
    return ProductEvent.from_product(
        ProductData(
            id=product_id,
            sku=f"SKU-{product_id}",
            name=name,
            category="Peripherals",
            price=Decimal(price),
            quantity=5,
            created_at=BASE,
            updated_at=updated,
        )
    )


def make_message(event=None, *, key=None, value=None, offset=0):
    if event is not None:
        key = event.key if key is None else key
        value = event.to_bytes() if value is None else value
    return IncomingMessage(topic="products", partition=0, offset=offset, key=key, value=value)


@pytest.fixture
def event_factory():
    return make_event


@pytest.fixture
def message_factory():
    return make_message
