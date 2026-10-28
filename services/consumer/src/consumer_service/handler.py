"""Business logic of one message: deserialize -> validate -> upsert."""

from __future__ import annotations

import logging
from typing import Protocol

from pydantic import ValidationError
from pymongo.errors import ConnectionFailure, OperationFailure, PyMongoError

from product_contracts import ProductEvent

from .messages import IncomingMessage, PermanentMessageError, TransientProcessingError
from .repository import UpsertResult

log = logging.getLogger(__name__)


class ProductStore(Protocol):
    def upsert(self, event: ProductEvent, message: IncomingMessage) -> UpsertResult: ...


def deserialize(message: IncomingMessage) -> ProductEvent:
    if message.value is None:
        raise PermanentMessageError("empty-payload", "message value is null")
    try:
        event = ProductEvent.from_bytes(message.value)
    except ValidationError as exc:
        raise PermanentMessageError("invalid-payload", str(exc)) from exc
    if message.key != event.key:
        raise PermanentMessageError("key-mismatch", f"key={message.key!r} but data.id={event.data.id}")
    return event


class ProductMessageHandler:
    def __init__(self, store: ProductStore) -> None:
        self._store = store

    def handle(self, message: IncomingMessage) -> tuple[ProductEvent, UpsertResult]:
        event = deserialize(message)
        try:
            result = self._store.upsert(event, message)
        except ConnectionFailure as exc:
            raise TransientProcessingError(f"MongoDB unavailable: {exc}") from exc
        except OperationFailure as exc:
            if exc.has_error_label("RetryableWriteError") or exc.has_error_label("TransientTransactionError"):
                raise TransientProcessingError(f"MongoDB transient failure: {exc}") from exc
            raise PermanentMessageError("rejected-by-mongodb", str(exc)) from exc
        except PyMongoError as exc:
            raise TransientProcessingError(f"MongoDB error: {exc}") from exc
        return event, result
