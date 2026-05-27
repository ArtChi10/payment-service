import logging

import pytest
from app.services import webhook as webhook_module
from app.services.webhook import WebhookDeliveryError, WebhookService


async def test_webhook_retry_succeeds_on_first_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    service = WebhookService()
    calls = []

    async def fake_send(url: str, payload: dict) -> None:
        calls.append((url, payload))

    async def fake_sleep(delay: int) -> None:
        raise AssertionError(f"unexpected sleep: {delay}")

    monkeypatch.setattr(service, "send", fake_send)
    monkeypatch.setattr(webhook_module.asyncio, "sleep", fake_sleep)

    await service.send_with_retry("https://example.com/webhook", {"status": "succeeded"})

    assert calls == [("https://example.com/webhook", {"status": "succeeded"})]


async def test_webhook_retry_succeeds_after_temporary_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = WebhookService()
    calls = []
    sleeps = []

    async def fake_send(url: str, payload: dict) -> None:
        calls.append((url, payload))
        if len(calls) == 1:
            raise WebhookDeliveryError("temporary failure")

    async def fake_sleep(delay: int) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(service, "send", fake_send)
    monkeypatch.setattr(webhook_module.asyncio, "sleep", fake_sleep)

    await service.send_with_retry("https://example.com/webhook", {"status": "succeeded"})

    assert len(calls) == 2
    assert sleeps == [1]


async def test_webhook_retry_raises_after_max_attempts(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    service = WebhookService()
    calls = []
    sleeps = []

    async def fake_send(url: str, payload: dict) -> None:
        calls.append((url, payload))
        raise WebhookDeliveryError("still failing")

    async def fake_sleep(delay: int) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(service, "send", fake_send)
    monkeypatch.setattr(webhook_module.asyncio, "sleep", fake_sleep)

    with (
        caplog.at_level(logging.WARNING),
        pytest.raises(WebhookDeliveryError, match="failed after retries"),
    ):
        await service.send_with_retry(
            "https://example.com/webhook",
            {"status": "succeeded"},
        )

    assert len(calls) == 3
    assert sleeps == [1, 2]
    assert caplog.messages == [
        "Webhook delivery failed on attempt 1: still failing",
        "Webhook delivery failed on attempt 2: still failing",
        "Webhook delivery failed on attempt 3: still failing",
    ]
