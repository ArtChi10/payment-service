"""add payment webhook delivery state

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-27 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "payments",
        sa.Column(
            "webhook_status",
            sa.Enum("pending", "sending", "delivered", "failed", native_enum=False, length=16),
            server_default="pending",
            nullable=False,
        ),
    )
    op.add_column(
        "payments",
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "payments",
        sa.Column("webhook_attempts", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column("payments", sa.Column("webhook_last_error", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("payments", "webhook_last_error")
    op.drop_column("payments", "webhook_attempts")
    op.drop_column("payments", "webhook_delivered_at")
    op.drop_column("payments", "webhook_status")
