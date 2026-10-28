"""Runs against the MongoDB from docker compose; skipped when it is not reachable."""

import os
import uuid

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from consumer_service.repository import MongoProductRepository, UpsertResult

pytestmark = pytest.mark.integration

MONGO_URI = os.getenv("TEST_MONGO_URI", "mongodb://root:root_pwd@127.0.0.1:27017/?authSource=admin")


@pytest.fixture
def repository():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500, tz_aware=True)
    try:
        client.admin.command("ping")
    except PyMongoError:
        pytest.skip("MongoDB is not running (docker compose up -d mongo)")
    database = client[f"test_{uuid.uuid4().hex[:8]}"]
    yield MongoProductRepository(database["products"])
    client.drop_database(database.name)
    client.close()


def count(repository, product_id):
    return repository._collection.count_documents({"_id": product_id})


def test_insert_update_and_duplicate(repository, event_factory, message_factory):
    created = event_factory(price="100.00")
    updated = event_factory(seconds=10, price="150.00")

    assert repository.upsert(created, message_factory(created)) is UpsertResult.INSERTED
    assert repository.upsert(updated, message_factory(updated)) is UpsertResult.UPDATED
    assert repository.upsert(updated, message_factory(updated)) is UpsertResult.SKIPPED

    document = repository._collection.find_one({"_id": 101})
    assert count(repository, 101) == 1
    assert str(document["price"]) == "150.00"


def test_stale_event_does_not_roll_back(repository, event_factory, message_factory):
    newer = event_factory(seconds=10, name="New name")
    older = event_factory(seconds=5, name="Old name")

    repository.upsert(newer, message_factory(newer))
    assert repository.upsert(older, message_factory(older)) is UpsertResult.SKIPPED

    assert repository._collection.find_one({"_id": 101})["name"] == "New name"


def test_changes_within_same_millisecond_are_applied(repository, event_factory, message_factory):
    first = event_factory(micros=100, name="first")
    second = event_factory(micros=200, name="second")

    repository.upsert(first, message_factory(first))
    assert repository.upsert(second, message_factory(second)) is UpsertResult.UPDATED
