from datetime import datetime
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Float
from sqlalchemy import String, JSON, TIMESTAMP

metadata = MetaData()

Category = Table(
    'category',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("title", String(80)),
)

Group = Table(
    'group',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("title", String(80)),
    Column("category", Integer, ForeignKey("category.id", ondelete='CASCADE')),
    Column("source_base_id", Integer, ForeignKey("source_base.id", ondelete='CASCADE')),
)

Report = Table(
    'report',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("category", Integer, ForeignKey("category.id", ondelete='CASCADE')),
    Column("source_base_id", Integer, ForeignKey("source_base.id", ondelete='CASCADE')),
)
