from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.models.enums import Currency, PaymentStatus, WebhookStatus
from app.models.payment import Payment


class PaymentCreate(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=12, decimal_places=2)
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetail(BaseModel):
    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    webhook_status: WebhookStatus
    webhook_delivered_at: datetime | None
    webhook_attempts: int
    webhook_last_error: str | None
    created_at: datetime
    processed_at: datetime | None

    model_config = ConfigDict(from_attributes=True)

    @classmethod
    def from_payment(cls, payment: Payment) -> "PaymentDetail":
        public_status = (
            PaymentStatus.PENDING
            if payment.status == PaymentStatus.PROCESSING
            else PaymentStatus(payment.status)
        )
        return cls(
            id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=public_status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            webhook_status=payment.webhook_status,
            webhook_delivered_at=payment.webhook_delivered_at,
            webhook_attempts=payment.webhook_attempts,
            webhook_last_error=payment.webhook_last_error,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class PaymentEvent(BaseModel):
    payment_id: UUID
    attempt: int = 1


class PaymentWebhookPayload(BaseModel):
    payment_id: UUID
    delivery_id: str
    status: PaymentStatus
    processed_at: datetime
