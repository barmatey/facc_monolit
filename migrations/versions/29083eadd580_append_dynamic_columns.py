"""append dynamic columns

Revision ID: 29083eadd580
Revises: 9cf49691f785
Create Date: 2023-07-19 13:00:28.280383

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '29083eadd580'
down_revision = '9cf49691f785'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('group', sa.Column('dynamic_columns', sa.JSON(), nullable=False))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('group', 'dynamic_columns')
    # ### end Alembic commands ###