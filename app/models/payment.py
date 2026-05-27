from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import JSON, DateTime, Enum, Integer, Numeric, String, Text, Uuid, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import Currency, PaymentStatus, WebhookStatus


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, native_enum=False, length=3),
        nullable=False,
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_: Mapped[dict] = mapped_column(
        "metadata",
        JSONB().with_variant(JSON(), "sqlite"),
        default=dict,
        nullable=False,
    )
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(PaymentStatus, native_enum=False, length=16),
        default=PaymentStatus.PENDING,
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    gateway_operation_id: Mapped[str] = mapped_column(
        String(255),
        default=lambda: f"gateway:{uuid4()}",
        unique=True,
        index=True,
    )
    webhook_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    webhook_status: Mapped[WebhookStatus] = mapped_column(
        Enum(WebhookStatus, native_enum=False, length=16),
        default=WebhookStatus.PENDING,
        server_default=WebhookStatus.PENDING.value,
        nullable=False,
    )
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    webhook_attempts: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default="0",
        nullable=False,
    )
    webhook_last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    processing_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    processing_attempts: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default="0",
        nullable=False,
    )
    webhook_sending_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
