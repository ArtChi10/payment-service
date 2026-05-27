from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
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

    async def get_batch(self, limit: int) -> list[OutboxEvent]:
        result = await self.session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.status.in_([OutboxStatus.PENDING, OutboxStatus.FAILED]))
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True),
        )
        return list(result.scalars().all())

    async def mark_published(self, event: OutboxEvent) -> None:
        event.status = OutboxStatus.PUBLISHED
        event.published_at = datetime.now(UTC)

    async def mark_failed(self, event: OutboxEvent) -> None:
        event.status = OutboxStatus.FAILED
        event.attempts += 1

    async def get_by_id(self, event_id: UUID) -> OutboxEvent | None:
        return await self.session.get(OutboxEvent, event_id)
