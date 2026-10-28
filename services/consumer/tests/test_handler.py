import pytest
from pymongo.errors import AutoReconnect, OperationFailure

from consumer_service.handler import ProductMessageHandler
from consumer_service.messages import PermanentMessageError, TransientProcessingError
from consumer_service.repository import UpsertResult


class FakeStore:
    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def upsert(self, event, message):
        if self.error:
            raise self.error
        self.calls.append(event)
        return UpsertResult.INSERTED


def test_valid_message_is_upserted(event_factory, message_factory):
    store = FakeStore()
    event = event_factory()

    handled, result = ProductMessageHandler(store).handle(message_factory(event))

    assert handled == event
    assert result is UpsertResult.INSERTED
    assert store.calls[0].data.id == 101


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (None, "empty-payload"),
        (b"not json", "invalid-payload"),
        (b'{"eventType":"product.updated","data":{"id":1}}', "invalid-payload"),
    ],
)
def test_broken_payload_is_permanent_error(message_factory, value, reason):
    with pytest.raises(PermanentMessageError) as info:
        ProductMessageHandler(FakeStore()).handle(message_factory(key=b"1", value=value))
    assert info.value.reason == reason


def test_key_must_match_product_id(event_factory, message_factory):
    message = message_factory(event_factory(product_id=5), key=b"6")

    with pytest.raises(PermanentMessageError) as info:
        ProductMessageHandler(FakeStore()).handle(message)
    assert info.value.reason == "key-mismatch"


def test_mongo_outage_is_transient(event_factory, message_factory):
    handler = ProductMessageHandler(FakeStore(AutoReconnect("connection refused")))

    with pytest.raises(TransientProcessingError):
        handler.handle(message_factory(event_factory()))


def test_document_rejected_by_mongo_is_permanent(event_factory, message_factory):
    error = OperationFailure("Document failed validation", code=121)

    with pytest.raises(PermanentMessageError) as info:
        ProductMessageHandler(FakeStore(error)).handle(message_factory(event_factory()))
    assert info.value.reason == "rejected-by-mongodb"
