import asyncio
import logging
from uuid import UUID

from app.core.config import settings
from app.db.session import async_session_factory
from app.messaging.broker import PAYMENTS_DLQ_ROUTING_KEY, PAYMENTS_NEW_ROUTING_KEY, broker
from app.models.enums import PaymentStatus
from app.repositories.payments import PaymentRepository
from app.schemas.payments import PaymentEvent, PaymentWebhookPayload
from app.services.gateway import PaymentGateway
from app.services.payments import PaymentService
from app.services.webhook import WebhookService

logger = logging.getLogger(__name__)


@broker.subscriber(PAYMENTS_NEW_ROUTING_KEY)
async def handle_payment_created(message: dict) -> None:
    event = PaymentEvent.model_validate(message)

    for attempt in range(event.attempt, settings.max_retry_attempts + 1):
        try:
            await process_payment(event.payment_id)
            return
        except Exception as exc:
            logger.exception("Payment processing failed on attempt %s", attempt)
            if attempt >= settings.max_retry_attempts:
                await broker.publish(
                    {
                        "payment_id": str(event.payment_id),
                        "reason": str(exc),
                        "attempts": attempt,
                    },
                    routing_key=PAYMENTS_DLQ_ROUTING_KEY,
                )
                return
            await asyncio.sleep(2 ** (attempt - 1))


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
