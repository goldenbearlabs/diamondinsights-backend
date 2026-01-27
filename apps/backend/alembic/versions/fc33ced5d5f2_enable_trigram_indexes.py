"""enable trigram indexes

Revision ID: fc33ced5d5f2
Revises: 1ad4f7c45426
Create Date: 2026-01-26 17:56:03.086275
"""

from typing import Sequence, Union

revision: str = "fc33ced5d5f2"
down_revision: Union[str, None] = "1ad4f7c45426"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # replaced by later migration that adds normalized search columns + indexes
    pass


def downgrade() -> None:
    pass
