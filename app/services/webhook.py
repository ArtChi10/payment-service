import asyncio
import logging
from typing import Any

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


class WebhookDeliveryError(RuntimeError):
    def __init__(self, message: str, attempts: int = 0) -> None:
        super().__init__(message)
        self.attempts = attempts


class WebhookService:
    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self.client = client

    async def send(self, url: str, payload: dict[str, Any]) -> None:
        if self.client:
            response = await self.client.post(url, json=payload)
        else:
            async with httpx.AsyncClient(timeout=settings.webhook_timeout_seconds) as client:
                response = await client.post(url, json=payload)

        if response.status_code >= 400:
            raise WebhookDeliveryError(f"Webhook returned {response.status_code}")

    async def send_with_retry(self, url: str, payload: dict[str, Any]) -> int:
        last_error: Exception | None = None
        for attempt in range(1, settings.max_retry_attempts + 1):
            try:
                await self.send(url, payload)
                return attempt
            except Exception as exc:
                last_error = exc
                logger.warning("Webhook delivery failed on attempt %s: %s", attempt, exc)
                if attempt < settings.max_retry_attempts:
                    await asyncio.sleep(2 ** (attempt - 1))

        raise WebhookDeliveryError(
            "Webhook delivery failed after retries",
            attempts=settings.max_retry_attempts,
        ) from last_error
