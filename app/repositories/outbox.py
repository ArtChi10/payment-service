from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.enums import OutboxStatus
from app.models.outbox import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, event: OutboxEvent) -> OutboxEvent:
        self.session.add(event)
        await self.session.flush()
        return event

    async def get_batch(self, limit: int, max_attempts: int) -> list[OutboxEvent]:
        result = await self.session.execute(
            select(OutboxEvent)
            .where(
                or_(
                    OutboxEvent.status == OutboxStatus.PENDING,
                    (OutboxEvent.status == OutboxStatus.FAILED)
                    & (OutboxEvent.attempts < max_attempts),
                ),
            )
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True),
        )
        return list(result.scalars().all())

    async def mark_published(self, event_id: UUID) -> None:
        event = await self.session.get(OutboxEvent, event_id)
        if event is None:
            return

        event.status = OutboxStatus.PUBLISHED
        event.published_at = datetime.now(UTC)

    async def mark_failed(self, event_id: UUID) -> None:
        event = await self.session.get(OutboxEvent, event_id)
        if event is None:
            return

        event.status = OutboxStatus.FAILED
        event.attempts += 1

    async def get_by_id(self, event_id: UUID) -> OutboxEvent | None:
        return await self.session.get(OutboxEvent, event_id)
