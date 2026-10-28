"""Entry point of the consumer service."""

from __future__ import annotations

import logging
import signal
import sys
import threading

from confluent_kafka import Consumer
from pymongo.errors import PyMongoError

from .config import get_settings
from .dlq import DeadLetterPublisher
from .handler import ProductMessageHandler
from .logging_config import configure_logging
from .repository import MongoProductRepository
from .runner import ConsumerRunner, build_consumer_config

log = logging.getLogger("consumer")


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level)

    stop = threading.Event()

    def handle_signal(signum: int, _frame: object) -> None:
        log.info("Received %s, shutting down gracefully", signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    mongo_client, repository = MongoProductRepository.connect(
        settings.mongo_uri, settings.mongo_database, settings.mongo_collection
    )
    try:
        repository.ping()
        log.info("Connected to MongoDB database '%s'", settings.mongo_database)
    except PyMongoError as exc:
        log.warning("MongoDB is not reachable yet (%s); messages will be retried", exc)

    consumer = Consumer(
        build_consumer_config(settings.kafka_bootstrap_servers, settings.group_id, settings.client_id)
    )
    dead_letters = DeadLetterPublisher.create(
        settings.kafka_bootstrap_servers, settings.kafka_dlq_topic, settings.client_id
    )
    runner = ConsumerRunner(
        consumer,
        ProductMessageHandler(repository),
        dead_letters,
        settings.kafka_topic,
        retry_initial=settings.retry_initial_seconds,
        retry_max=settings.retry_max_seconds,
    )

    log.info(
        "Consumer started: Kafka topic '%s' (group '%s') -> MongoDB '%s.%s'",
        settings.kafka_topic,
        settings.group_id,
        settings.mongo_database,
        settings.mongo_collection,
    )
    try:
        runner.run(stop)
    finally:
        consumer.close()
        dead_letters.close()
        mongo_client.close()
        log.info("Consumer stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
