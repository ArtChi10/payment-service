import asyncio
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from app.db.base import Base
from app.db.session import get_session
from app.main import app
from app.models.outbox import OutboxEvent
from app.models.payment import Payment
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

VALID_PAYMENT_BODY = {
    "amount": "42.00",
    "currency": "USD",
    "description": "Order",
    "metadata": {"source": "test"},
    "webhook_url": "https://example.com/webhook",
}


@pytest.fixture(autouse=True)
def override_session(session_factory):
    async def _get_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = _get_session
    yield
    app.dependency_overrides.clear()


async def test_create_payment_requires_api_key_header():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/payments",
            headers={"Idempotency-Key": "missing-api-key"},
            json=VALID_PAYMENT_BODY,
        )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "X-API-Key"]


async def test_create_payment_rejects_invalid_api_key():
    headers = {
        "X-API-Key": "wrong-key",
        "Idempotency-Key": "invalid-api-key",
    }

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/v1/payments", headers=headers, json=VALID_PAYMENT_BODY)

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid API key"


async def test_create_payment_requires_idempotency_key():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/payments",
            headers={"X-API-Key": "change-me"},
            json=VALID_PAYMENT_BODY,
        )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "Idempotency-Key"]


async def test_create_and_get_payment():
    headers = {
        "X-API-Key": "change-me",
        "Idempotency-Key": "api-idem-1",
    }

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post("/api/v1/payments", headers=headers, json=VALID_PAYMENT_BODY)
        payment_id = created.json()["payment_id"]
        fetched = await client.get(
            f"/api/v1/payments/{payment_id}", headers={"X-API-Key": "change-me"}
        )

    assert created.status_code == 202
    assert fetched.status_code == 200
    assert fetched.json()["status"] == "pending"
    assert fetched.json()["metadata"] == {"source": "test"}


async def test_get_missing_payment_returns_404():
    missing_payment_id = uuid4()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(
            f"/api/v1/payments/{missing_payment_id}",
            headers={"X-API-Key": "change-me"},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "Payment not found"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("currency", "GBP"),
        ("amount", "0.00"),
        ("amount", "-1.00"),
        ("webhook_url", "not-a-url"),
    ],
)
async def test_create_payment_validates_payload(field: str, value: str):
    body = {**VALID_PAYMENT_BODY, field: value}
    headers = {
        "X-API-Key": "change-me",
        "Idempotency-Key": f"validation-{field}-{value}",
    }

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/v1/payments", headers=headers, json=body)

    assert response.status_code == 422


async def test_concurrent_create_payment_is_idempotent(tmp_path):
    database_path = tmp_path / "concurrent.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    file_session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async def _get_file_session() -> AsyncIterator[AsyncSession]:
        async with file_session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = _get_file_session

    headers = {
        "X-API-Key": "change-me",
        "Idempotency-Key": "concurrent-idem-1",
    }

    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            first, second = await asyncio.gather(
                client.post("/api/v1/payments", headers=headers, json=VALID_PAYMENT_BODY),
                client.post("/api/v1/payments", headers=headers, json=VALID_PAYMENT_BODY),
            )

        assert first.status_code == 202
        assert second.status_code == 202
        assert first.json()["payment_id"] == second.json()["payment_id"]

        async with file_session_factory() as session:
            payments = (
                (
                    await session.execute(
                        select(Payment).where(Payment.idempotency_key == "concurrent-idem-1"),
                    )
                )
                .scalars()
                .all()
            )
            outbox_events = (await session.execute(select(OutboxEvent))).scalars().all()

        assert len(payments) == 1
        assert len(outbox_events) == 1
    finally:
        await engine.dispose()
