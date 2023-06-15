"""init

Revision ID: 7aa3d829bf3d
Revises: 
Create Date: 2023-06-15 17:31:25.725758

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7aa3d829bf3d'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('category',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('value', sa.String(length=80), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('value')
    )
    op.create_table('source',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('title', sa.String(length=80), nullable=False),
    sa.Column('total_start_date', sa.TIMESTAMP(), nullable=False),
    sa.Column('total_end_date', sa.TIMESTAMP(), nullable=False),
    sa.Column('wcols', sa.JSON(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('group',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('title', sa.String(length=80), nullable=False),
    sa.Column('category_id', sa.Integer(), nullable=False),
    sa.Column('source_id', sa.Integer(), nullable=False),
    sa.Column('sheet_id', sa.String(length=30), nullable=False),
    sa.ForeignKeyConstraint(['category_id'], ['category.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['source_id'], ['source.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('sheet_id')
    )
    op.create_table('wire',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('date', sa.TIMESTAMP(), nullable=False),
    sa.Column('sender', sa.Float(), nullable=False),
    sa.Column('receiver', sa.Float(), nullable=False),
    sa.Column('debit', sa.Float(), nullable=False),
    sa.Column('credit', sa.Float(), nullable=False),
    sa.Column('subconto_first', sa.String(length=800), nullable=True),
    sa.Column('subconto_second', sa.String(length=800), nullable=True),
    sa.Column('comment', sa.String(length=800), nullable=True),
    sa.Column('source_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['source_id'], ['source.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('report',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('title', sa.String(length=80), nullable=False),
    sa.Column('category_id', sa.Integer(), nullable=False),
    sa.Column('group_id', sa.Integer(), nullable=False),
    sa.Column('source_id', sa.Integer(), nullable=False),
    sa.Column('sheet', sa.String(length=30), nullable=False),
    sa.ForeignKeyConstraint(['category_id'], ['category.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['group_id'], ['group.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['source_id'], ['source.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('sheet')
    )
    op.create_table('interval',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('total_start_date', sa.TIMESTAMP(), nullable=True),
    sa.Column('total_end_date', sa.TIMESTAMP(), nullable=True),
    sa.Column('start_date', sa.TIMESTAMP(), nullable=False),
    sa.Column('end_date', sa.TIMESTAMP(), nullable=False),
    sa.Column('period_year', sa.Integer(), nullable=False),
    sa.Column('period_month', sa.Integer(), nullable=False),
    sa.Column('period_day', sa.Integer(), nullable=False),
    sa.Column('report_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['report_id'], ['report.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('interval')
    op.drop_table('report')
    op.drop_table('wire')
    op.drop_table('group')
    op.drop_table('source')
    op.drop_table('category')
    # ### end Alembic commands ###
