import asyncio
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import PaymentStatus
from app.models.outbox import OutboxEvent
from app.models.payment import Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas.payments import PaymentCreate

IDEMPOTENCY_CONFLICT_LOOKUP_ATTEMPTS = 10
IDEMPOTENCY_CONFLICT_LOOKUP_DELAY_SECONDS = 0.01


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.payments = PaymentRepository(session)
        self.outbox = OutboxRepository(session)

    async def create_payment(self, data: PaymentCreate, idempotency_key: str) -> Payment:
        try:
            async with self.session.begin():
                existing = await self.payments.get_by_idempotency_key(idempotency_key)
                if existing:
                    return existing

                payment = Payment(
                    amount=data.amount,
                    currency=data.currency,
                    description=data.description,
                    metadata_=data.metadata,
                    status=PaymentStatus.PENDING,
                    idempotency_key=idempotency_key,
                    webhook_url=str(data.webhook_url),
                )
                await self.payments.add(payment)
                await self.outbox.add(
                    OutboxEvent(
                        event_type="payments.new",
                        routing_key="payments.new",
                        payload={
                            "payment_id": str(payment.id),
                            "attempt": 1,
                        },
                    ),
                )
                return payment
        except IntegrityError:
            await self.session.rollback()
            for _ in range(IDEMPOTENCY_CONFLICT_LOOKUP_ATTEMPTS):
                existing = await self.payments.get_by_idempotency_key(idempotency_key)
                if existing:
                    return existing
                await asyncio.sleep(IDEMPOTENCY_CONFLICT_LOOKUP_DELAY_SECONDS)
            raise

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        return await self.payments.get_by_id(payment_id)

    async def claim_for_processing(self, payment_id: UUID) -> Payment | None:
        return await self.payments.claim_for_processing(payment_id)

    async def release_processing_claim(self, payment_id: UUID) -> None:
        await self.payments.release_processing_claim(payment_id)

    async def mark_processed(self, payment: Payment, status: PaymentStatus) -> Payment:
        payment.status = status
        payment.processed_at = datetime.now(UTC)
        await self.session.flush()
        return payment

    async def mark_processed_by_id(self, payment_id: UUID, status: PaymentStatus) -> Payment:
        payment = await self.payments.mark_processed(payment_id, status)
        if payment is None:
            raise LookupError(f"Payment {payment_id} not found")
        payment.processed_at = datetime.now(UTC)
        await self.session.flush()
        return payment

    async def claim_webhook_delivery(self, payment_id: UUID) -> Payment | None:
        return await self.payments.claim_webhook_delivery(payment_id)

    async def mark_webhook_delivered(self, payment_id: UUID, attempts: int) -> None:
        await self.payments.mark_webhook_delivered(payment_id, attempts, datetime.now(UTC))

    async def mark_webhook_failed(self, payment_id: UUID, attempts: int, error: str) -> None:
        await self.payments.mark_webhook_failed(payment_id, attempts, error[:4000])
