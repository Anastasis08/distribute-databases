import threading

from consumer_service.handler import ProductMessageHandler
from consumer_service.messages import TransientProcessingError
from consumer_service.repository import UpsertResult
from consumer_service.runner import ConsumerRunner


class FakeConsumer:
    def __init__(self):
        self.commits = []
        self.seeks = []

    def commit(self, message, asynchronous):
        self.commits.append(message)

    def seek(self, partition):
        self.seeks.append((partition.topic, partition.partition, partition.offset))


class FlakyStore:
    def __init__(self, failures):
        self.failures = failures

    def upsert(self, event, message):
        if self.failures:
            self.failures -= 1
            raise TransientProcessingError("mongo down")
        return UpsertResult.INSERTED


class FakeDlq:
    def __init__(self):
        self.messages = []

    def publish(self, message, reason, detail):
        self.messages.append((message, reason))


def make_runner(store, consumer, dlq):
    return ConsumerRunner(
        consumer, ProductMessageHandler(store), dlq, "products", retry_initial=0.001, retry_max=0.001
    )


def test_offset_committed_after_successful_write(event_factory, message_factory):
    consumer, dlq = FakeConsumer(), FakeDlq()
    message = message_factory(event_factory(), offset=7)

    make_runner(FlakyStore(0), consumer, dlq).process(message, "record-7", threading.Event())

    assert consumer.commits == ["record-7"]
    assert consumer.seeks == []


def test_transient_failure_seeks_back_and_does_not_commit(event_factory, message_factory):
    consumer, dlq = FakeConsumer(), FakeDlq()
    runner = make_runner(FlakyStore(1), consumer, dlq)
    message = message_factory(event_factory(), offset=7)

    runner.process(message, "record-7", threading.Event())
    assert consumer.commits == []
    assert consumer.seeks == [("products", 0, 7)]

    runner.process(message, "record-7", threading.Event())
    assert consumer.commits == ["record-7"]


def test_poison_message_goes_to_dlq_and_is_committed(message_factory):
    consumer, dlq = FakeConsumer(), FakeDlq()
    message = message_factory(key=b"1", value=b"{broken", offset=3)

    make_runner(FlakyStore(0), consumer, dlq).process(message, "record-3", threading.Event())

    assert [reason for _, reason in dlq.messages] == ["invalid-payload"]
    assert consumer.commits == ["record-3"]
