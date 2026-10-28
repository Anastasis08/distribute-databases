"""MongoDB read model of the product catalog with idempotent, version-guarded upserts."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from enum import StrEnum

from bson.decimal128 import Decimal128
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from product_contracts import ProductEvent

from .messages import IncomingMessage

log = logging.getLogger(__name__)


class UpsertResult(StrEnum):
    INSERTED = "inserted"
    UPDATED = "updated"
    SKIPPED = "skipped (duplicate or stale version)"


class MongoProductRepository:
    def __init__(self, collection: Collection) -> None:
        self._collection = collection

    @classmethod
    def connect(cls, uri: str, database: str, collection: str) -> tuple[MongoClient, MongoProductRepository]:
        client: MongoClient = MongoClient(
            uri,
            appname="product-consumer",
            tz_aware=True,
            serverSelectionTimeoutMS=5000,
            retryWrites=True,
            w="majority",
        )
        return client, cls(client[database][collection])

    def ping(self) -> None:
        self._collection.database.client.admin.command("ping")

    def upsert(self, event: ProductEvent, message: IncomingMessage) -> UpsertResult:
        """Insert the document or update it, but only with a strictly newer version.

        Filter {_id, sync.version < incoming}:
          * no document          -> upsert inserts a new one (_id = SQL id)
          * older document       -> matched and replaced with the new state
          * same/newer document  -> not matched; the upsert collides on _id
                                    (DuplicateKeyError) and the event is skipped.
        Re-delivered or out-of-date messages therefore never create copies nor roll data back.
        """
        product = event.data
        document = {
            "sku": product.sku,
            "name": product.name,
            "category": product.category,
            "price": Decimal128(product.price),
            "quantity": product.quantity,
            "createdAt": product.created_at,
            "updatedAt": product.updated_at,
            "sync": {
                "version": product.version,
                "eventId": str(event.event_id),
                "eventType": event.event_type,
                "syncedAt": datetime.now(UTC),
                "kafka": {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                },
            },
        }
        try:
            result = self._collection.update_one(
                {"_id": product.id, "sync.version": {"$lt": product.version}},
                {"$set": document},
                upsert=True,
            )
        except DuplicateKeyError:
            return UpsertResult.SKIPPED
        if result.upserted_id is not None:
            return UpsertResult.INSERTED
        return UpsertResult.UPDATED
