import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from producer_service.publisher import PublishError
from producer_service.repository import Watermark
from producer_service.sync import ChangeSyncService
from product_contracts import ProductData

BASE = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)


def product(product_id: int, seconds: int, created_seconds: int | None = None) -> ProductData:
    created = BASE + timedelta(seconds=created_seconds if created_seconds is not None else seconds)
    return ProductData(
        id=product_id,
        sku=f"SKU-{product_id}",
        name=f"Product {product_id}",
        category="Test",
        price=Decimal("10.00"),
        quantity=1,
        created_at=created,
        updated_at=BASE + timedelta(seconds=seconds),
    )


class FakeSource:
    def __init__(self, rows):
        self.rows = rows
        self.requested_after = []

    def fetch_changes(self, after, limit, settle_delay):
        self.requested_after.append(after)
        newer = [r for r in self.rows if (r.updated_at, r.id) > (after.updated_at, after.product_id)]
        return sorted(newer, key=lambda r: (r.updated_at, r.id))[:limit]


class FakeCheckpoints:
    def __init__(self):
        self.value = Watermark.initial()
        self.saves = 0

    def load(self):
        return self.value

    def save(self, watermark):
        self.value = watermark
        self.saves += 1


class FakePublisher:
    def __init__(self, fail=False):
        self.fail = fail
        self.events = []

    def publish_batch(self, events, timeout):
        if self.fail:
            raise PublishError("broker down")
        self.events.extend(events)
        return len(events)


def make_service(source, checkpoints, publisher, batch_size=100):
    return ChangeSyncService(
        source,
        checkpoints,
        publisher,
        batch_size=batch_size,
        settle_delay=timedelta(0),
        poll_interval=0.01,
        flush_timeout=1,
    )


def test_publishes_changes_and_advances_checkpoint():
    source = FakeSource([product(2, 5), product(1, 3)])
    checkpoints, publisher = FakeCheckpoints(), FakePublisher()

    assert make_service(source, checkpoints, publisher).sync_once() == 2

    assert [e.data.id for e in publisher.events] == [1, 2]
    assert all(e.key == str(e.data.id).encode() for e in publisher.events)
    assert checkpoints.value == Watermark(BASE + timedelta(seconds=5), 2)


def test_second_poll_without_changes_publishes_nothing():
    source = FakeSource([product(1, 3)])
    checkpoints, publisher = FakeCheckpoints(), FakePublisher()
    service = make_service(source, checkpoints, publisher)

    service.sync_once()
    assert service.sync_once() == 0
    assert len(publisher.events) == 1


def test_updated_row_is_published_again_as_update():
    source = FakeSource([product(1, 3)])
    checkpoints, publisher = FakeCheckpoints(), FakePublisher()
    service = make_service(source, checkpoints, publisher)
    service.sync_once()

    source.rows = [product(1, 10, created_seconds=3)]
    service.sync_once()

    assert [e.event_type for e in publisher.events] == ["product.created", "product.updated"]


def test_checkpoint_not_advanced_when_kafka_fails():
    checkpoints = FakeCheckpoints()
    service = make_service(FakeSource([product(1, 3)]), checkpoints, FakePublisher(fail=True))

    with pytest.raises(PublishError):
        service.sync_once()

    assert checkpoints.saves == 0
    assert checkpoints.value == Watermark.initial()


def test_rows_with_same_timestamp_are_paged_by_id():
    source = FakeSource([product(i, 3) for i in range(1, 6)])
    checkpoints, publisher = FakeCheckpoints(), FakePublisher()
    service = make_service(source, checkpoints, publisher, batch_size=2)

    while service.sync_once():
        pass

    assert [e.data.id for e in publisher.events] == [1, 2, 3, 4, 5]


def test_run_forever_survives_failures_and_stops():
    stop = threading.Event()
    publisher = FakePublisher(fail=True)
    service = make_service(FakeSource([product(1, 3)]), FakeCheckpoints(), publisher)
    service.MAX_BACKOFF_SECONDS = 0.01

    thread = threading.Thread(target=service.run_forever, args=(stop,))
    thread.start()
    publisher.fail = False
    stop.wait(0.2)
    stop.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert [e.data.id for e in publisher.events] == [1]
