"""Entry point: `product-producer run | republish --id N | reset-checkpoint`."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from datetime import timedelta

from product_contracts import ProductEvent

from .config import get_settings
from .logging_config import configure_logging
from .publisher import KafkaEventPublisher
from .repository import CheckpointRepository, ProductRepository, create_db_engine
from .sync import ChangeSyncService

log = logging.getLogger("producer")


def _install_signal_handlers(stop: threading.Event) -> None:
    def handle(signum: int, _frame: object) -> None:
        log.info("Received %s, shutting down gracefully", signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGINT, handle)
    signal.signal(signal.SIGTERM, handle)


def run() -> int:
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    publisher = KafkaEventPublisher.create(
        settings.kafka_bootstrap_servers, settings.kafka_topic, settings.client_id
    )
    service = ChangeSyncService(
        ProductRepository(engine),
        CheckpointRepository(engine, settings.checkpoint_name),
        publisher,
        batch_size=settings.batch_size,
        settle_delay=timedelta(milliseconds=settings.settle_delay_ms),
        poll_interval=settings.poll_interval_seconds,
        flush_timeout=settings.flush_timeout_seconds,
    )
    stop = threading.Event()
    _install_signal_handlers(stop)

    log.info("Producer started: PostgreSQL -> Kafka topic '%s'", settings.kafka_topic)
    publisher.ensure_topic_exists()
    try:
        service.run_forever(stop)
    finally:
        publisher.close()
        engine.dispose()
        log.info("Producer stopped")
    return 0


def republish(product_ids: list[int]) -> int:
    """Re-send the current state of products (used to demonstrate idempotent consumption)."""
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    publisher = KafkaEventPublisher.create(
        settings.kafka_bootstrap_servers, settings.kafka_topic, settings.client_id
    )
    try:
        products = ProductRepository(engine).get_current(product_ids)
        missing = sorted(set(product_ids) - {p.id for p in products})
        if missing:
            log.warning("Products not found in SQL: %s", missing)
        events = [ProductEvent.from_product(product) for product in products]
        count = publisher.publish_batch(events, settings.flush_timeout_seconds)
        log.info("Re-published %d event(s)", count)
    finally:
        publisher.close()
        engine.dispose()
    return 0


def reset_checkpoint() -> int:
    settings = get_settings()
    engine = create_db_engine(settings.database_url)
    CheckpointRepository(engine, settings.checkpoint_name).reset()
    engine.dispose()
    log.info("Checkpoint '%s' removed: all products will be published again", settings.checkpoint_name)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="product-producer", description=__doc__)
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("run", help="poll PostgreSQL and publish changes (default)")
    republish_cmd = commands.add_parser("republish", help="send current state of products again")
    republish_cmd.add_argument("--id", dest="ids", type=int, action="append", required=True)
    commands.add_parser("reset-checkpoint", help="forget progress and re-publish everything")
    args = parser.parse_args(argv)

    configure_logging(get_settings().log_level)
    if args.command == "republish":
        return republish(args.ids)
    if args.command == "reset-checkpoint":
        return reset_checkpoint()
    return run()


if __name__ == "__main__":
    sys.exit(main())
