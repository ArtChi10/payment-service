from datetime import datetime
from uuid import UUID

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import PaymentStatus, WebhookStatus
from app.models.payment import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key),
        )
        return result.scalar_one_or_none()

    async def add(self, payment: Payment) -> Payment:
        self.session.add(payment)
        await self.session.flush()
        return payment

    async def claim_for_processing(self, payment_id: UUID) -> Payment | None:
        result = await self.session.execute(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.processed_at.is_(None),
                Payment.status == PaymentStatus.PENDING,
            )
            .values(status=PaymentStatus.PROCESSING)
            .returning(Payment),
        )
        return result.scalar_one_or_none()

    async def release_processing_claim(self, payment_id: UUID) -> None:
        await self.session.execute(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.processed_at.is_(None),
                Payment.status == PaymentStatus.PROCESSING,
            )
            .values(status=PaymentStatus.PENDING),
        )

    async def mark_processed(self, payment_id: UUID, status: PaymentStatus) -> Payment | None:
        result = await self.session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(status=status)
            .returning(Payment),
        )
        return result.scalar_one_or_none()

    async def claim_webhook_delivery(self, payment_id: UUID) -> Payment | None:
        result = await self.session.execute(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.processed_at.is_not(None),
                Payment.webhook_status.in_((WebhookStatus.PENDING, WebhookStatus.FAILED)),
            )
            .values(webhook_status=WebhookStatus.SENDING)
            .returning(Payment),
        )
        return result.scalar_one_or_none()

    async def mark_webhook_delivered(
        self,
        payment_id: UUID,
        attempts: int,
        delivered_at: datetime,
    ) -> Payment | None:
        result = await self.session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(
                webhook_status=WebhookStatus.DELIVERED,
                webhook_delivered_at=delivered_at,
                webhook_attempts=Payment.webhook_attempts + attempts,
                webhook_last_error=None,
            )
            .returning(Payment),
        )
        return result.scalar_one_or_none()

    async def mark_webhook_failed(
        self,
        payment_id: UUID,
        attempts: int,
        error: str,
    ) -> Payment | None:
        result = await self.session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(
                webhook_status=WebhookStatus.FAILED,
                webhook_attempts=Payment.webhook_attempts + attempts,
                webhook_last_error=error,
            )
            .returning(Payment),
        )
        return result.scalar_one_or_none()

    async def get_processed_for_webhook(self, payment_id: UUID) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(
                Payment.id == payment_id,
                Payment.processed_at.is_not(None),
                or_(
                    Payment.webhook_status == WebhookStatus.PENDING,
                    Payment.webhook_status == WebhookStatus.FAILED,
                ),
            ),
        )
        return result.scalar_one_or_none()
