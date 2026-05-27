from uuid import UUID

import pytest
from app.messaging import broker as broker_module
from app.messaging import handlers
from faststream.exceptions import RejectMessage


def test_payments_topology_contract() -> None:
    assert broker_module.PAYMENTS_EXCHANGE.name == "payments"
    assert broker_module.PAYMENTS_EXCHANGE.durable is True

    assert broker_module.PAYMENTS_NEW_QUEUE.name == "payments.new"
    assert broker_module.PAYMENTS_NEW_QUEUE.durable is True
    assert broker_module.PAYMENTS_NEW_QUEUE.routing() == "payments.new"
    assert broker_module.PAYMENTS_NEW_QUEUE.arguments["x-dead-letter-exchange"] == "payments.dlx"
    assert broker_module.PAYMENTS_NEW_QUEUE.arguments["x-dead-letter-routing-key"] == "payments.dlq"

    assert broker_module.PAYMENTS_RETRY_EXCHANGE.name == "payments.retry"
    assert broker_module.PAYMENTS_RETRY_EXCHANGE.durable is True

    assert broker_module.PAYMENTS_RETRY_QUEUE.name == "payments.retry"
    assert broker_module.PAYMENTS_RETRY_QUEUE.durable is True
    assert broker_module.PAYMENTS_RETRY_QUEUE.routing() == "payments.retry"
    assert broker_module.PAYMENTS_RETRY_QUEUE.arguments["x-dead-letter-exchange"] == "payments"
    assert (
        broker_module.PAYMENTS_RETRY_QUEUE.arguments["x-dead-letter-routing-key"]
        == "payments.new"
    )

    assert broker_module.PAYMENTS_DLX.name == "payments.dlx"
    assert broker_module.PAYMENTS_DLX.durable is True

    assert broker_module.PAYMENTS_DLQ_QUEUE.name == "payments.dlq"
    assert broker_module.PAYMENTS_DLQ_QUEUE.durable is True
    assert broker_module.PAYMENTS_DLQ_QUEUE.routing() == "payments.dlq"


async def test_declare_rabbitmq_topology_declares_bindings(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeBroker:
        def __init__(self) -> None:
            self.exchanges = []
            self.queues = []
            self.binds = []

        async def declare_exchange(self, exchange):
            self.exchanges.append(exchange)
            return exchange

        async def declare_queue(self, queue):
            self.queues.append(queue)

            class FakeQueue:
                async def bind(self, exchange, **kwargs):
                    self.binds.append((queue, exchange, kwargs))

            fake_queue = FakeQueue()
            fake_queue.binds = self.binds
            return fake_queue

    fake_broker = FakeBroker()
    monkeypatch.setattr(broker_module, "broker", fake_broker)

    await broker_module.declare_rabbitmq_topology()

    assert fake_broker.exchanges == [
        broker_module.PAYMENTS_EXCHANGE,
        broker_module.PAYMENTS_RETRY_EXCHANGE,
        broker_module.PAYMENTS_DLX,
    ]
    assert fake_broker.queues == [
        broker_module.PAYMENTS_NEW_QUEUE,
        broker_module.PAYMENTS_RETRY_QUEUE,
        broker_module.PAYMENTS_DLQ_QUEUE,
    ]
    assert fake_broker.binds == [
        (
            broker_module.PAYMENTS_NEW_QUEUE,
            broker_module.PAYMENTS_EXCHANGE,
            {
                "routing_key": "payments.new",
                "arguments": None,
                "timeout": None,
                "robust": True,
            },
        ),
        (
            broker_module.PAYMENTS_RETRY_QUEUE,
            broker_module.PAYMENTS_RETRY_EXCHANGE,
            {
                "routing_key": "payments.retry",
                "arguments": None,
                "timeout": None,
                "robust": True,
            },
        ),
        (
            broker_module.PAYMENTS_DLQ_QUEUE,
            broker_module.PAYMENTS_DLX,
            {
                "routing_key": "payments.dlq",
                "arguments": None,
                "timeout": None,
                "robust": True,
            },
        ),
    ]


async def test_handler_routes_retry_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeBroker:
        def __init__(self) -> None:
            self.messages = []

        async def publish(self, message: dict, **kwargs: object) -> None:
            self.messages.append((message, kwargs))

    fake_broker = FakeBroker()

    async def fail_processing(payment_id: UUID) -> None:
        raise RuntimeError("gateway unavailable")

    monkeypatch.setattr(handlers, "process_payment", fail_processing)
    monkeypatch.setattr(handlers, "broker", fake_broker)

    message = {
        "payment_id": "00000000-0000-0000-0000-000000000001",
        "attempt": 1,
    }

    await handlers.handle_payment_created(message)

    assert fake_broker.messages == [
        (
            {
                "payment_id": "00000000-0000-0000-0000-000000000001",
                "attempt": 2,
            },
            {
                "exchange": broker_module.PAYMENTS_RETRY_EXCHANGE,
                "routing_key": "payments.retry",
                "expiration": 1,
                "persist": True,
            },
        )
    ]


async def test_handler_rejects_to_dlx_after_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = []

    class FakeBroker:
        def __init__(self) -> None:
            self.messages = []

        async def publish(self, message: dict, **kwargs: object) -> None:
            self.messages.append((message, kwargs))

    fake_broker = FakeBroker()

    async def fail_processing(payment_id: UUID) -> None:
        attempts.append(payment_id)
        raise RuntimeError("gateway unavailable")

    monkeypatch.setattr(handlers, "process_payment", fail_processing)
    monkeypatch.setattr(handlers, "broker", fake_broker)

    message = {
        "payment_id": "00000000-0000-0000-0000-000000000001",
        "attempt": 3,
    }

    with pytest.raises(RejectMessage) as exc_info:
        await handlers.handle_payment_created(message)

    assert exc_info.value.extra_options == {"requeue": False}
    assert len(attempts) == 1
    assert fake_broker.messages == []
