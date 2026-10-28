from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from product_contracts import ProductData, ProductEvent, message_key

CREATED = datetime(2026, 10, 13, 12, 0, 0, 123456, tzinfo=UTC)


def make_product(**overrides) -> ProductData:
    values = {
        "id": 101,
        "sku": "KB-1",
        "name": "Keyboard",
        "category": "Peripherals",
        "price": Decimal("1200.00"),
        "quantity": 5,
        "created_at": CREATED,
        "updated_at": CREATED,
    }
    values.update(overrides)
    return ProductData(**values)


def test_event_round_trips_through_json():
    event = ProductEvent.from_product(make_product())

    restored = ProductEvent.from_bytes(event.to_bytes())

    assert restored == event


def test_json_uses_camel_case_and_exact_decimal():
    payload = ProductEvent.from_product(make_product()).to_bytes().decode()

    assert '"eventType":"product.created"' in payload
    assert '"updatedAt":"2026-10-13T12:00:00.123456Z"' in payload
    assert '"price":"1200.00"' in payload


def test_key_is_the_stable_product_id():
    event = ProductEvent.from_product(make_product(id=42))

    assert event.key == b"42" == message_key(42)


def test_event_type_reflects_insert_or_update():
    later = CREATED + timedelta(seconds=5)

    assert ProductEvent.from_product(make_product()).event_type == "product.created"
    assert ProductEvent.from_product(make_product(updated_at=later)).event_type == "product.updated"


def test_version_distinguishes_microseconds():
    first = make_product()
    second = make_product(updated_at=CREATED + timedelta(microseconds=1))

    assert second.version - first.version == 1


def test_timestamps_are_normalized_to_utc():
    kyiv = timezone(timedelta(hours=3))
    product = make_product(updated_at=CREATED.astimezone(kyiv))

    assert product.updated_at.tzinfo == UTC
    assert product.updated_at == CREATED


@pytest.mark.parametrize(
    "overrides",
    [
        {"price": Decimal("-1")},
        {"price": Decimal("1.234")},
        {"quantity": -1},
        {"id": 0},
        {"updated_at": datetime(2026, 1, 1)},
    ],
)
def test_invalid_products_are_rejected(overrides):
    with pytest.raises(ValidationError):
        make_product(**overrides)
