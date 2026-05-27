from collections.abc import AsyncIterator

import pytest
from app.db.session import get_session
from app.main import app
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture(autouse=True)
def override_session(session_factory):
    async def _get_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = _get_session
    yield
    app.dependency_overrides.clear()


async def test_create_payment_requires_api_key():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/v1/payments", json={})

    assert response.status_code == 422


async def test_create_and_get_payment():
    headers = {
        "X-API-Key": "change-me",
        "Idempotency-Key": "api-idem-1",
    }
    body = {
        "amount": "42.00",
        "currency": "USD",
        "description": "Order",
        "metadata": {"source": "test"},
        "webhook_url": "https://example.com/webhook",
    }

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post("/api/v1/payments", headers=headers, json=body)
        payment_id = created.json()["payment_id"]
        fetched = await client.get(
            f"/api/v1/payments/{payment_id}", headers={"X-API-Key": "change-me"}
        )

    assert created.status_code == 202
    assert fetched.status_code == 200
    assert fetched.json()["status"] == "pending"
    assert fetched.json()["metadata"] == {"source": "test"}
