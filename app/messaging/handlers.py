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
from app.models.enums import PaymentStatus, WebhookStatus
from app.repositories.payments import PaymentRepository
from app.schemas.payments import PaymentEvent, PaymentWebhookPayload
from app.services.gateway import PaymentGateway
from app.services.payments import PaymentService
from app.services.webhook import WebhookDeliveryError, WebhookService

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
        async with session.begin():
            service = PaymentService(session)
            payment = await service.claim_for_processing(payment_id)
            should_process = payment is not None

            if payment is None:
                payment = await PaymentRepository(session).get_by_id(payment_id)

        if payment is None:
            raise LookupError(f"Payment {payment_id} not found")

        if should_process:
            try:
                status = await gateway.process(payment)
            except Exception:
                async with session.begin():
                    await PaymentService(session).release_processing_claim(payment_id)
                raise

            async with session.begin():
                payment = await PaymentService(session).mark_processed_by_id(payment.id, status)

        if payment.processed_at is None:
            logger.info("Payment %s is already being processed by another consumer", payment_id)
            return

        if payment.webhook_status == WebhookStatus.DELIVERED:
            logger.info("Payment %s webhook is already delivered", payment_id)
            return

        async with session.begin():
            payment = await PaymentService(session).claim_webhook_delivery(payment_id)

        if payment is None:
            logger.info("Payment %s webhook delivery is already claimed or completed", payment_id)
            return

        webhook_payload = PaymentWebhookPayload(
            payment_id=payment.id,
            delivery_id=f"payment:{payment.id}:webhook",
            status=PaymentStatus(payment.status),
            processed_at=payment.processed_at,
        ).model_dump(mode="json")

        try:
            attempts = await webhook.send_with_retry(payment.webhook_url, webhook_payload)
            attempts = attempts or 1
        except WebhookDeliveryError as exc:
            async with session.begin():
                await PaymentService(session).mark_webhook_failed(
                    payment.id,
                    exc.attempts or settings.max_retry_attempts,
                    str(exc),
                )
            raise
        except Exception as exc:
            async with session.begin():
                await PaymentService(session).mark_webhook_failed(payment.id, 1, str(exc))
            raise

        async with session.begin():
            await PaymentService(session).mark_webhook_delivered(payment.id, attempts)
