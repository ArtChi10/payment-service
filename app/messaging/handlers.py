import logging
from uuid import UUID

from faststream.exceptions import RejectMessage

from app.core.config import settings
from app.db.session import async_session_factory
from app.messaging.broker import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    PAYMENTS_RETRY_EXCHANGE,
    PAYMENTS_RETRY_ROUTING_KEY,
    broker,
)
from app.models.enums import PaymentStatus
from app.repositories.payments import PaymentRepository
from app.schemas.payments import PaymentEvent, PaymentWebhookPayload
from app.services.gateway import PaymentGateway
from app.services.payments import PaymentService
from app.services.webhook import WebhookService

logger = logging.getLogger(__name__)


@broker.subscriber(PAYMENTS_NEW_QUEUE, PAYMENTS_EXCHANGE)
async def handle_payment_created(message: dict) -> None:
    event = PaymentEvent.model_validate(message)

    try:
        await process_payment(event.payment_id)
    except Exception as exc:
        logger.exception("Payment processing failed on attempt %s", event.attempt)
        if event.attempt >= settings.max_retry_attempts:
            raise RejectMessage(requeue=False) from exc

        await broker.publish(
            {
                "payment_id": str(event.payment_id),
                "attempt": event.attempt + 1,
            },
            exchange=PAYMENTS_RETRY_EXCHANGE,
            routing_key=PAYMENTS_RETRY_ROUTING_KEY,
            expiration=2 ** (event.attempt - 1),
            persist=True,
        )


async def process_payment(payment_id: UUID) -> None:
    gateway = PaymentGateway()
    webhook = WebhookService()

    async with async_session_factory() as session:
        payment = await PaymentRepository(session).get_by_id(payment_id)
        if payment is None:
            raise LookupError(f"Payment {payment_id} not found")

        if payment.processed_at is None:
            status = await gateway.process(payment)
            await PaymentService(session).mark_processed(payment, status)
            await session.commit()

        if payment.processed_at is None:
            raise RuntimeError(f"Payment {payment_id} was not processed")

        webhook_payload = PaymentWebhookPayload(
            payment_id=payment.id,
            status=PaymentStatus(payment.status),
            processed_at=payment.processed_at,
        ).model_dump(mode="json")
        await webhook.send(payment.webhook_url, webhook_payload)
