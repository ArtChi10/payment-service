import asyncio
import random

from app.core.config import settings
from app.models.enums import PaymentStatus
from app.models.payment import Payment


class PaymentGateway:
    async def process(self, payment: Payment) -> PaymentStatus:
        delay = random.uniform(
            settings.payment_gateway_min_delay_seconds,
            settings.payment_gateway_max_delay_seconds,
        )
        await asyncio.sleep(delay)
        return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED
