"""add payment processing leases

Revision ID: 0003
Revises: 0002
Create Date: 2026-05-27 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "payments",
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "payments",
        sa.Column("processing_attempts", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "payments",
        sa.Column("webhook_sending_started_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("payments", "webhook_sending_started_at")
    op.drop_column("payments", "processing_attempts")
    op.drop_column("payments", "processing_started_at")
