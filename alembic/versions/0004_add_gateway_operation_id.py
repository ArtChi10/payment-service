"""add gateway operation id

Revision ID: 0004
Revises: 0003
Create Date: 2026-05-27 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "payments",
        sa.Column("gateway_operation_id", sa.String(length=255), nullable=True),
    )
    op.execute(
        "UPDATE payments SET gateway_operation_id = 'gateway:' || id::text "
        "WHERE gateway_operation_id IS NULL",
    )
    op.alter_column("payments", "gateway_operation_id", nullable=False)
    op.create_index(
        "ix_payments_gateway_operation_id",
        "payments",
        ["gateway_operation_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_payments_gateway_operation_id", table_name="payments")
    op.drop_column("payments", "gateway_operation_id")
