from app.messaging.broker import PAYMENTS_EXCHANGE
from app.models.enums import OutboxStatus
from app.models.outbox import OutboxEvent
from app.services.outbox import OutboxPublisher


class FakeBroker:
    def __init__(self, should_fail: bool = False) -> None:
        self.messages = []
        self.should_fail = should_fail

    async def publish(self, message: dict, routing_key: str, **kwargs: object) -> None:
        if self.should_fail:
            raise RuntimeError("broker unavailable")

        self.messages.append((routing_key, message, kwargs))


async def test_outbox_publishes_and_marks_event(session_factory):
    async with session_factory() as session, session.begin():
        event = OutboxEvent(
            event_type="payments.new",
            routing_key="payments.new",
            payload={"payment_id": "00000000-0000-0000-0000-000000000001", "attempt": 1},
        )
        session.add(event)

    broker = FakeBroker()
    published = await OutboxPublisher(session_factory, broker).publish_pending()

    async with session_factory() as session:
        stored = await session.get(OutboxEvent, event.id)

    assert published == 1
    assert broker.messages == [
        (
            "payments.new",
            event.payload,
            {"exchange": PAYMENTS_EXCHANGE, "persist": True},
        )
    ]
    assert stored.status == OutboxStatus.PUBLISHED
    assert stored.published_at is not None
    assert stored.attempts == 0


async def test_outbox_failed_publish_increments_attempts(session_factory):
    async with session_factory() as session, session.begin():
        event = OutboxEvent(
            event_type="payments.new",
            routing_key="payments.new",
            payload={"payment_id": "00000000-0000-0000-0000-000000000001", "attempt": 1},
        )
        session.add(event)

    broker = FakeBroker(should_fail=True)
    published = await OutboxPublisher(session_factory, broker).publish_pending()

    async with session_factory() as session:
        stored = await session.get(OutboxEvent, event.id)

    assert published == 0
    assert broker.messages == []
    assert stored.status == OutboxStatus.FAILED
    assert stored.attempts == 1
    assert stored.published_at is None


async def test_outbox_retries_failed_event(session_factory):
    async with session_factory() as session, session.begin():
        event = OutboxEvent(
            event_type="payments.new",
            routing_key="payments.new",
            payload={"payment_id": "00000000-0000-0000-0000-000000000001", "attempt": 1},
            status=OutboxStatus.FAILED,
            attempts=1,
        )
        session.add(event)

    broker = FakeBroker()
    published = await OutboxPublisher(
        session_factory,
        broker,
        max_publish_attempts=3,
    ).publish_pending()

    async with session_factory() as session:
        stored = await session.get(OutboxEvent, event.id)

    assert published == 1
    assert len(broker.messages) == 1
    assert stored.status == OutboxStatus.PUBLISHED
    assert stored.attempts == 1


async def test_outbox_does_not_retry_failed_event_after_attempt_limit(session_factory):
    async with session_factory() as session, session.begin():
        event = OutboxEvent(
            event_type="payments.new",
            routing_key="payments.new",
            payload={"payment_id": "00000000-0000-0000-0000-000000000001", "attempt": 1},
            status=OutboxStatus.FAILED,
            attempts=3,
        )
        session.add(event)

    broker = FakeBroker()
    published = await OutboxPublisher(
        session_factory,
        broker,
        max_publish_attempts=3,
    ).publish_pending()

    async with session_factory() as session:
        stored = await session.get(OutboxEvent, event.id)

    assert published == 0
    assert broker.messages == []
    assert stored.status == OutboxStatus.FAILED
    assert stored.attempts == 3
