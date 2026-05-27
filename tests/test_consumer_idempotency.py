from datetime import UTC, datetime
from decimal import Decimal

import pytest
from app.messaging import handlers
from app.models.enums import Currency, PaymentStatus
from app.models.payment import Payment


async def test_processed_payment_does_not_call_gateway_again(
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    async with session_factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("42.00"),
            currency=Currency.USD,
            description="Processed order",
            metadata_={"source": "test"},
            status=PaymentStatus.SUCCEEDED,
            idempotency_key="processed-payment",
            webhook_url="https://example.com/webhook",
            processed_at=datetime.now(UTC),
        )
        session.add(payment)

    gateway_calls = []
    webhook_calls = []

    class FakeGateway:
        async def process(self, payment: Payment) -> PaymentStatus:
            gateway_calls.append(payment.id)
            return PaymentStatus.SUCCEEDED

    class FakeWebhook:
        async def send(self, url: str, payload: dict) -> None:
            webhook_calls.append((url, payload))

    monkeypatch.setattr(handlers, "async_session_factory", session_factory)
    monkeypatch.setattr(handlers, "PaymentGateway", FakeGateway)
    monkeypatch.setattr(handlers, "WebhookService", FakeWebhook)

    await handlers.process_payment(payment.id)

    assert gateway_calls == []
    assert len(webhook_calls) == 1
    assert webhook_calls[0][0] == "https://example.com/webhook"
    assert webhook_calls[0][1]["payment_id"] == str(payment.id)
    assert webhook_calls[0][1]["status"] == "succeeded"
