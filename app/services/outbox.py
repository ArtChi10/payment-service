import asyncio
import logging
from typing import Protocol

from sqlalchemy.ext.asyncio import async_sessionmaker

from app.core.config import settings
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)


class BrokerPublisher(Protocol):
    async def publish(self, message: dict, routing_key: str, **kwargs: object) -> object:
        pass


class OutboxPublisher:
    def __init__(
        self,
        session_factory: async_sessionmaker,
        broker: BrokerPublisher,
        batch_size: int | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.broker = broker
        self.batch_size = batch_size or settings.outbox_batch_size

    async def publish_pending(self) -> int:
        async with self.session_factory() as session, session.begin():
            repository = OutboxRepository(session)
            events = await repository.get_batch(self.batch_size)
            published_count = 0

            for event in events:
                try:
                    await self.broker.publish(event.payload, routing_key=event.routing_key)
                except Exception:
                    logger.exception("Failed to publish outbox event %s", event.id)
                    await repository.mark_failed(event)
                else:
                    await repository.mark_published(event)
                    published_count += 1

            return published_count


async def run_outbox_loop(publisher: OutboxPublisher) -> None:
    while True:
        try:
            await publisher.publish_pending()
        except Exception:
            logger.exception("Outbox loop failed")
        await asyncio.sleep(settings.outbox_poll_interval_seconds)
