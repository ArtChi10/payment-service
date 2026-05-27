from app.models.enums import OutboxStatus
from app.models.outbox import OutboxEvent
from app.services.outbox import OutboxPublisher


class FakeBroker:
    def __init__(self) -> None:
        self.messages = []

    async def publish(self, message: dict, routing_key: str, **kwargs: object) -> None:
        self.messages.append((routing_key, message))


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
    assert broker.messages == [("payments.new", event.payload)]
    assert stored.status == OutboxStatus.PUBLISHED
    assert stored.published_at is not None
