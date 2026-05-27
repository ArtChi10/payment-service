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

    async def claim_for_processing(
        self,
        payment_id: UUID,
        *,
        now: datetime,
        lease_expired_before: datetime,
    ) -> Payment | None:
        result = await self.session.execute(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.processed_at.is_(None),
                or_(
                    Payment.status == PaymentStatus.PENDING,
                    (
                        (Payment.status == PaymentStatus.PROCESSING)
                        & (
                            Payment.processing_started_at.is_(None)
                            | (Payment.processing_started_at < lease_expired_before)
                        )
                    ),
                ),
            )
            .values(
                status=PaymentStatus.PROCESSING,
                processing_started_at=now,
                processing_attempts=Payment.processing_attempts + 1,
            )
            .execution_options(synchronize_session=False)
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
            .values(status=PaymentStatus.PENDING)
            .execution_options(synchronize_session=False),
        )

    async def mark_processed(self, payment_id: UUID, status: PaymentStatus) -> Payment | None:
        result = await self.session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(status=status)
            .execution_options(synchronize_session=False)
            .returning(Payment),
        )
        return result.scalar_one_or_none()

    async def claim_webhook_delivery(
        self,
        payment_id: UUID,
        *,
        now: datetime,
        lease_expired_before: datetime,
    ) -> Payment | None:
        result = await self.session.execute(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.processed_at.is_not(None),
                or_(
                    Payment.webhook_status.in_((WebhookStatus.PENDING, WebhookStatus.FAILED)),
                    (
                        (Payment.webhook_status == WebhookStatus.SENDING)
                        & (
                            Payment.webhook_sending_started_at.is_(None)
                            | (Payment.webhook_sending_started_at < lease_expired_before)
                        )
                    ),
                ),
            )
            .values(
                webhook_status=WebhookStatus.SENDING,
                webhook_sending_started_at=now,
            )
            .execution_options(synchronize_session=False)
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
            .execution_options(synchronize_session=False)
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
            .execution_options(synchronize_session=False)
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
