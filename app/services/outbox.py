import asyncio
import logging
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import settings
from app.messaging.broker import PAYMENTS_EXCHANGE
from app.models.outbox import OutboxEvent
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)

OUTBOX_MAX_PUBLISH_ATTEMPTS = 3


class BrokerPublisher(Protocol):
    async def publish(self, message: dict, routing_key: str, **kwargs: object) -> object:
        pass


class OutboxPublisher:
    def __init__(
        self,
        session_factory: async_sessionmaker,
        broker: BrokerPublisher,
        batch_size: int | None = None,
        max_publish_attempts: int = OUTBOX_MAX_PUBLISH_ATTEMPTS,
    ) -> None:
        self.session_factory = session_factory
        self.broker = broker
        self.batch_size = batch_size or settings.outbox_batch_size
        self.max_publish_attempts = max_publish_attempts

    async def publish_pending(self) -> int:
        events = await self._get_publishable_events()
        published_count = 0

        for event in events:
            try:
                await self.broker.publish(
                    event.payload,
                    exchange=PAYMENTS_EXCHANGE,
                    routing_key=event.routing_key,
                    persist=True,
                )
            except Exception:
                logger.exception("Failed to publish outbox event %s", event.id)
                await self._mark_failed(event.id)
            else:
                await self._mark_published(event.id)
                published_count += 1

        return published_count

    async def _get_publishable_events(self) -> list[OutboxEvent]:
        async with self.session_factory() as session, session.begin():
            repository = OutboxRepository(session)
            return await repository.get_batch(self.batch_size, self.max_publish_attempts)

    async def _mark_published(self, event_id: UUID) -> None:
        async with self.session_factory() as session, session.begin():
            repository = OutboxRepository(session)
            await repository.mark_published(event_id)

    async def _mark_failed(self, event_id: UUID) -> None:
        async with self.session_factory() as session, session.begin():
            repository = OutboxRepository(session)
            await repository.mark_failed(event_id)


async def run_outbox_loop(publisher: OutboxPublisher) -> None:
    while True:
        try:
            await publisher.publish_pending()
        except Exception:
            logger.exception("Outbox loop failed")
        await asyncio.sleep(settings.outbox_poll_interval_seconds)
