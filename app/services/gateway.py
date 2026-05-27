import asyncio
import logging
import random

from app.core.config import settings
from app.models.enums import PaymentStatus
from app.models.payment import Payment

logger = logging.getLogger(__name__)


class PaymentGateway:
    async def process(self, payment: Payment, operation_id: str) -> PaymentStatus:
        logger.info("Processing payment %s with gateway operation id %s", payment.id, operation_id)
        delay = random.uniform(
            settings.payment_gateway_min_delay_seconds,
            settings.payment_gateway_max_delay_seconds,
        )
        await asyncio.sleep(delay)
        return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED
