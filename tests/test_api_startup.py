from collections.abc import Coroutine
from typing import Any

from app import main as main_module


async def test_api_lifespan_declares_topology_before_outbox(monkeypatch) -> None:
    events = []

    class FakeBroker:
        async def connect(self) -> None:
            events.append("connect")

        async def close(self) -> None:
            events.append("close")

    class FakeOutboxPublisher:
        def __init__(self, session_factory: object, broker: FakeBroker) -> None:
            events.append("create_publisher")

    class FakeTask:
        def cancel(self) -> None:
            events.append("cancel_outbox_task")

    async def fake_declare_rabbitmq_topology() -> None:
        events.append("declare_topology")

    async def fake_run_outbox_loop(publisher: FakeOutboxPublisher) -> None:
        return None

    def fake_create_task(coro: Coroutine[Any, Any, None]) -> FakeTask:
        events.append("start_outbox_loop")
        coro.close()
        return FakeTask()

    monkeypatch.setattr(main_module, "broker", FakeBroker())
    monkeypatch.setattr(main_module, "declare_rabbitmq_topology", fake_declare_rabbitmq_topology)
    monkeypatch.setattr(main_module, "OutboxPublisher", FakeOutboxPublisher)
    monkeypatch.setattr(main_module, "run_outbox_loop", fake_run_outbox_loop)
    monkeypatch.setattr(main_module.asyncio, "create_task", fake_create_task)

    async with main_module.lifespan(main_module.app):
        assert events == [
            "connect",
            "declare_topology",
            "create_publisher",
            "start_outbox_loop",
        ]

    assert events == [
        "connect",
        "declare_topology",
        "create_publisher",
        "start_outbox_loop",
        "cancel_outbox_task",
        "close",
    ]
