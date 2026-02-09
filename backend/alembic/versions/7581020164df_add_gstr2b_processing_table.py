"""add_gstr2b_processing_table

Revision ID: 7581020164df
Revises: 248644e207e7
Create Date: 2026-01-10 17:49:14.056265

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7581020164df'
down_revision: Union[str, Sequence[str], None] = '248644e207e7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'gstr2b_processing',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('purchase_file_path', sa.String(length=500), nullable=False),
        sa.Column('purchase_filename', sa.String(length=255), nullable=False),
        sa.Column('gstr2b_file_paths', sa.JSON(), nullable=False),
        sa.Column('gstr2b_filenames', sa.JSON(), nullable=False),
        sa.Column('status', sa.Enum('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', name='gstr2bprocessingstatus'), nullable=False),
        sa.Column('task_id', sa.String(length=255), nullable=True),
        sa.Column('result_file_path', sa.String(length=500), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('processing_metadata', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('completed_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_gstr2b_processing_id'), 'gstr2b_processing', ['id'], unique=False)
    op.create_index(op.f('ix_gstr2b_processing_user_id'), 'gstr2b_processing', ['user_id'], unique=False)
    op.create_index(op.f('ix_gstr2b_processing_task_id'), 'gstr2b_processing', ['task_id'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_gstr2b_processing_task_id'), table_name='gstr2b_processing')
    op.drop_index(op.f('ix_gstr2b_processing_user_id'), table_name='gstr2b_processing')
    op.drop_index(op.f('ix_gstr2b_processing_id'), table_name='gstr2b_processing')
    op.drop_table('gstr2b_processing')
    op.execute('DROP TYPE gstr2bprocessingstatus')
