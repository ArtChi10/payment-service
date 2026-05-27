from decimal import Decimal

from app.models.outbox import OutboxEvent
from app.schemas.payments import PaymentCreate
from app.services.payments import PaymentService
from sqlalchemy import select


async def test_create_payment_is_idempotent(session):
    payload = PaymentCreate(
        amount=Decimal("100.00"),
        currency="RUB",
        description="Order 1",
        metadata={"order_id": "1"},
        webhook_url="https://example.com/payment-webhook",
    )

    first = await PaymentService(session).create_payment(payload, "idem-1")
    second = await PaymentService(session).create_payment(payload, "idem-1")

    result = await session.execute(select(OutboxEvent))
    events = result.scalars().all()

    assert second.id == first.id
    assert first.gateway_operation_id == f"gateway:{first.id}"
    assert second.gateway_operation_id == first.gateway_operation_id
    assert len(events) == 1
