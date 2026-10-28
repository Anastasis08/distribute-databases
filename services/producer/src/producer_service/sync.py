"""Polling change-detection loop: PostgreSQL rows -> ProductEvent -> Kafka."""

from __future__ import annotations

import logging
import threading
from datetime import timedelta
from typing import Protocol

from sqlalchemy.exc import SQLAlchemyError

from product_contracts import ProductData, ProductEvent

from .publisher import PublishError
from .repository import Watermark

log = logging.getLogger(__name__)


class ChangeSource(Protocol):
    def fetch_changes(self, after: Watermark, limit: int, settle_delay: timedelta) -> list[ProductData]: ...


class CheckpointStore(Protocol):
    def load(self) -> Watermark: ...
    def save(self, watermark: Watermark) -> None: ...


class EventPublisher(Protocol):
    def publish_batch(self, events: list[ProductEvent], timeout: float) -> int: ...


class ChangeSyncService:
    """At-least-once publisher.

    The checkpoint is advanced only after Kafka acknowledged the whole batch. A crash between
    the two steps re-sends the batch, which the idempotent consumer tolerates.
    """

    MAX_BACKOFF_SECONDS = 30.0

    def __init__(
        self,
        source: ChangeSource,
        checkpoints: CheckpointStore,
        publisher: EventPublisher,
        *,
        batch_size: int,
        settle_delay: timedelta,
        poll_interval: float,
        flush_timeout: float,
    ) -> None:
        self._source = source
        self._checkpoints = checkpoints
        self._publisher = publisher
        self._batch_size = batch_size
        self._settle_delay = settle_delay
        self._poll_interval = poll_interval
        self._flush_timeout = flush_timeout

    def sync_once(self) -> int:
        watermark = self._checkpoints.load()
        changes = self._source.fetch_changes(watermark, self._batch_size, self._settle_delay)
        if not changes:
            return 0

        log.info("Detected %d new/changed product(s) after %s", len(changes), watermark)
        events = [ProductEvent.from_product(product) for product in changes]
        self._publisher.publish_batch(events, self._flush_timeout)

        new_watermark = Watermark.of(changes[-1])
        self._checkpoints.save(new_watermark)
        log.info(
            "Checkpoint advanced to updated_at=%s id=%s",
            new_watermark.updated_at.isoformat(),
            new_watermark.product_id,
        )
        return len(changes)

    def run_forever(self, stop: threading.Event) -> None:
        log.info(
            "Polling for changes every %.1fs (batch=%d, settle delay=%s)",
            self._poll_interval,
            self._batch_size,
            self._settle_delay,
        )
        failures = 0
        while not stop.is_set():
            try:
                published = self.sync_once()
                failures = 0
            except (SQLAlchemyError, PublishError) as exc:
                failures += 1
                delay = min(self._poll_interval * 2**failures, self.MAX_BACKOFF_SECONDS)
                log.error("Sync iteration failed (%s: %s); retrying in %.1fs", type(exc).__name__, exc, delay)
                stop.wait(delay)
                continue
            if published < self._batch_size:
                stop.wait(self._poll_interval)
