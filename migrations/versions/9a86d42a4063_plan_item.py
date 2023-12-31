"""plan item

Revision ID: 9a86d42a4063
Revises: 9489d8be8808
Create Date: 2023-08-17 17:42:16.495358

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9a86d42a4063'
down_revision = '9489d8be8808'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('plan_item',
    sa.Column('sender', sa.Float(), nullable=False),
    sa.Column('receiver', sa.Float(), nullable=False),
    sa.Column('sub1', sa.String(length=800), nullable=True),
    sa.Column('sub2', sa.String(length=800), nullable=True),
    sa.Column('source_id', sa.Integer(), nullable=False),
    sa.Column('updated_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.ForeignKeyConstraint(['source_id'], ['source.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('plan_item')
    # ### end Alembic commands ###
