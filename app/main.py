import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import router as api_router
from app.core.config import settings
from app.core.logging import configure_logging
from app.db.session import async_session_factory
from app.messaging.broker import broker, declare_rabbitmq_topology
from app.services.outbox import OutboxPublisher, run_outbox_loop

configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.connect()
    await declare_rabbitmq_topology()
    publisher = OutboxPublisher(async_session_factory, broker)
    outbox_task = asyncio.create_task(run_outbox_loop(publisher))
    try:
        yield
    finally:
        outbox_task.cancel()
        await broker.close()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(api_router)


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}
