import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from app.messaging import handlers
from app.models.enums import Currency, PaymentStatus, WebhookStatus
from app.models.payment import Payment
from app.services.webhook import WebhookDeliveryError


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
    webhook_retry_calls = []
    webhook_direct_calls = []

    class FakeGateway:
        async def process(self, payment: Payment, operation_id: str) -> PaymentStatus:
            gateway_calls.append((payment.id, operation_id))
            return PaymentStatus.SUCCEEDED

    class FakeWebhook:
        async def send(self, url: str, payload: dict) -> None:
            webhook_direct_calls.append((url, payload))

        async def send_with_retry(self, url: str, payload: dict) -> None:
            webhook_retry_calls.append((url, payload))

    monkeypatch.setattr(handlers, "async_session_factory", session_factory)
    monkeypatch.setattr(handlers, "PaymentGateway", FakeGateway)
    monkeypatch.setattr(handlers, "WebhookService", FakeWebhook)

    await handlers.process_payment(payment.id)

    assert gateway_calls == []
    assert webhook_direct_calls == []
    assert len(webhook_retry_calls) == 1
    assert webhook_retry_calls[0][0] == "https://example.com/webhook"
    assert webhook_retry_calls[0][1]["payment_id"] == str(payment.id)
    assert webhook_retry_calls[0][1]["delivery_id"] == f"payment:{payment.id}:webhook"
    assert webhook_retry_calls[0][1]["status"] == "succeeded"


async def test_parallel_duplicate_events_call_gateway_once(
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    async with session_factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("42.00"),
            currency=Currency.USD,
            description="Pending order",
            metadata_={"source": "test"},
            status=PaymentStatus.PENDING,
            idempotency_key="parallel-payment",
            webhook_url="https://example.com/webhook",
        )
        session.add(payment)

    gateway_calls = []
    webhook_calls = []

    class FakeGateway:
        async def process(self, payment: Payment, operation_id: str) -> PaymentStatus:
            gateway_calls.append((payment.id, operation_id))
            await asyncio.sleep(0.05)
            return PaymentStatus.SUCCEEDED

    class FakeWebhook:
        async def send_with_retry(self, url: str, payload: dict) -> int:
            webhook_calls.append((url, payload))
            return 1

    monkeypatch.setattr(handlers, "async_session_factory", session_factory)
    monkeypatch.setattr(handlers, "PaymentGateway", FakeGateway)
    monkeypatch.setattr(handlers, "WebhookService", FakeWebhook)

    await asyncio.gather(
        handlers.process_payment(payment.id),
        handlers.process_payment(payment.id),
    )

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)

    assert gateway_calls == [(payment.id, payment.gateway_operation_id)]
    assert len(webhook_calls) == 1
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_status == WebhookStatus.DELIVERED
    assert stored.webhook_attempts == 1
    assert stored.processing_attempts == 1
    assert stored.processing_started_at is not None


async def test_active_processing_lease_does_not_call_gateway(
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    async with session_factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("42.00"),
            currency=Currency.USD,
            description="Active processing order",
            metadata_={"source": "test"},
            status=PaymentStatus.PROCESSING,
            idempotency_key="active-processing-payment",
            webhook_url="https://example.com/webhook",
            processing_started_at=datetime.now(UTC),
            processing_attempts=1,
        )
        session.add(payment)

    gateway_calls = []

    class FakeGateway:
        async def process(self, payment: Payment, operation_id: str) -> PaymentStatus:
            gateway_calls.append((payment.id, operation_id))
            return PaymentStatus.SUCCEEDED

    monkeypatch.setattr(handlers, "async_session_factory", session_factory)
    monkeypatch.setattr(handlers, "PaymentGateway", FakeGateway)

    await handlers.process_payment(payment.id)

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)

    assert gateway_calls == []
    assert stored.status == PaymentStatus.PROCESSING
    assert stored.processing_attempts == 1
    assert stored.processed_at is None


async def test_stale_processing_lease_allows_gateway_retry(
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    async with session_factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("42.00"),
            currency=Currency.USD,
            description="Stale processing order",
            metadata_={"source": "test"},
            status=PaymentStatus.PROCESSING,
            idempotency_key="stale-processing-payment",
            webhook_url="https://example.com/webhook",
            processing_started_at=datetime.now(UTC) - timedelta(seconds=120),
            processing_attempts=1,
        )
        session.add(payment)

    gateway_calls = []
    webhook_calls = []

    class FakeGateway:
        async def process(self, payment: Payment, operation_id: str) -> PaymentStatus:
            gateway_calls.append((payment.id, operation_id))
            return PaymentStatus.SUCCEEDED

    class FakeWebhook:
        async def send_with_retry(self, url: str, payload: dict) -> int:
            webhook_calls.append((url, payload))
            return 1

    monkeypatch.setattr(handlers, "async_session_factory", session_factory)
    monkeypatch.setattr(handlers, "PaymentGateway", FakeGateway)
    monkeypatch.setattr(handlers, "WebhookService", FakeWebhook)

    await handlers.process_payment(payment.id)

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)

    assert gateway_calls == [(payment.id, payment.gateway_operation_id)]
    assert len(webhook_calls) == 1
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.processing_attempts == 2
    assert stored.processed_at is not None
    assert stored.webhook_status == WebhookStatus.DELIVERED


async def test_delivered_webhook_is_not_sent_again(
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    async with session_factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("42.00"),
            currency=Currency.USD,
            description="Delivered order",
            metadata_={"source": "test"},
            status=PaymentStatus.SUCCEEDED,
            idempotency_key="delivered-payment",
            webhook_url="https://example.com/webhook",
            processed_at=datetime.now(UTC),
            webhook_status=WebhookStatus.DELIVERED,
            webhook_delivered_at=datetime.now(UTC),
            webhook_attempts=1,
        )
        session.add(payment)

    gateway_calls = []
    webhook_calls = []

    class FakeGateway:
        async def process(self, payment: Payment, operation_id: str) -> PaymentStatus:
            gateway_calls.append((payment.id, operation_id))
            return PaymentStatus.SUCCEEDED

    class FakeWebhook:
        async def send_with_retry(self, url: str, payload: dict) -> int:
            webhook_calls.append((url, payload))
            return 1

    monkeypatch.setattr(handlers, "async_session_factory", session_factory)
    monkeypatch.setattr(handlers, "PaymentGateway", FakeGateway)
    monkeypatch.setattr(handlers, "WebhookService", FakeWebhook)

    await handlers.process_payment(payment.id)

    assert gateway_calls == []
    assert webhook_calls == []


async def test_active_sending_lease_does_not_send_webhook_again(
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    async with session_factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("42.00"),
            currency=Currency.USD,
            description="Active sending order",
            metadata_={"source": "test"},
            status=PaymentStatus.SUCCEEDED,
            idempotency_key="active-sending-payment",
            webhook_url="https://example.com/webhook",
            processed_at=datetime.now(UTC),
            webhook_status=WebhookStatus.SENDING,
            webhook_sending_started_at=datetime.now(UTC),
        )
        session.add(payment)

    webhook_calls = []

    class FakeWebhook:
        async def send_with_retry(self, url: str, payload: dict) -> int:
            webhook_calls.append((url, payload))
            return 1

    monkeypatch.setattr(handlers, "async_session_factory", session_factory)
    monkeypatch.setattr(handlers, "WebhookService", FakeWebhook)

    await handlers.process_payment(payment.id)

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)

    assert webhook_calls == []
    assert stored.webhook_status == WebhookStatus.SENDING
    assert stored.webhook_attempts == 0


async def test_stale_sending_lease_allows_webhook_retry(
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    async with session_factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("42.00"),
            currency=Currency.USD,
            description="Stale sending order",
            metadata_={"source": "test"},
            status=PaymentStatus.SUCCEEDED,
            idempotency_key="stale-sending-payment",
            webhook_url="https://example.com/webhook",
            processed_at=datetime.now(UTC),
            webhook_status=WebhookStatus.SENDING,
            webhook_sending_started_at=datetime.now(UTC) - timedelta(seconds=120),
        )
        session.add(payment)

    webhook_calls = []

    class FakeWebhook:
        async def send_with_retry(self, url: str, payload: dict) -> int:
            webhook_calls.append((url, payload))
            return 1

    monkeypatch.setattr(handlers, "async_session_factory", session_factory)
    monkeypatch.setattr(handlers, "WebhookService", FakeWebhook)

    await handlers.process_payment(payment.id)

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)

    assert len(webhook_calls) == 1
    assert webhook_calls[0][1]["delivery_id"] == f"payment:{payment.id}:webhook"
    assert stored.webhook_status == WebhookStatus.DELIVERED
    assert stored.webhook_attempts == 1
    assert stored.webhook_delivered_at is not None


async def test_successful_webhook_updates_delivery_state(
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    async with session_factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("42.00"),
            currency=Currency.USD,
            description="Webhook order",
            metadata_={"source": "test"},
            status=PaymentStatus.SUCCEEDED,
            idempotency_key="webhook-success",
            webhook_url="https://example.com/webhook",
            processed_at=datetime.now(UTC),
            webhook_status=WebhookStatus.PENDING,
        )
        session.add(payment)

    payloads = []

    class FakeWebhook:
        async def send_with_retry(self, url: str, payload: dict) -> int:
            payloads.append(payload)
            return 2

    monkeypatch.setattr(handlers, "async_session_factory", session_factory)
    monkeypatch.setattr(handlers, "WebhookService", FakeWebhook)

    await handlers.process_payment(payment.id)

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)

    assert payloads[0]["delivery_id"] == f"payment:{payment.id}:webhook"
    assert stored.webhook_status == WebhookStatus.DELIVERED
    assert stored.webhook_attempts == 2
    assert stored.webhook_delivered_at is not None
    assert stored.webhook_last_error is None


async def test_failed_webhook_updates_delivery_state(
    monkeypatch: pytest.MonkeyPatch,
    session_factory,
) -> None:
    async with session_factory() as session, session.begin():
        payment = Payment(
            amount=Decimal("42.00"),
            currency=Currency.USD,
            description="Webhook failure order",
            metadata_={"source": "test"},
            status=PaymentStatus.SUCCEEDED,
            idempotency_key="webhook-failure",
            webhook_url="https://example.com/webhook",
            processed_at=datetime.now(UTC),
            webhook_status=WebhookStatus.PENDING,
        )
        session.add(payment)

    class FakeWebhook:
        async def send_with_retry(self, url: str, payload: dict) -> int:
            raise WebhookDeliveryError("temporary outage", attempts=3)

    monkeypatch.setattr(handlers, "async_session_factory", session_factory)
    monkeypatch.setattr(handlers, "WebhookService", FakeWebhook)

    with pytest.raises(WebhookDeliveryError, match="temporary outage"):
        await handlers.process_payment(payment.id)

    async with session_factory() as session:
        stored = await session.get(Payment, payment.id)

    assert stored.webhook_status == WebhookStatus.FAILED
    assert stored.webhook_attempts == 3
    assert stored.webhook_last_error == "temporary outage"
