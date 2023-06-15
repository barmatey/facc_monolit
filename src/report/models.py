from datetime import datetime
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Float
from sqlalchemy import String, JSON, TIMESTAMP

metadata = MetaData()

Category = Table(
    'category',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("value", String(80), nullable=False, unique=True),
)

Interval = Table(
    'interval',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("start_date", TIMESTAMP, nullable=False),
    Column("end_date", TIMESTAMP, nullable=False),
    Column("period_year", Integer, nullable=False),
    Column("period_month", Integer, nullable=False),
    Column("period_day", Integer, nullable=False),
    Column("report", Integer, ForeignKey("report.id", ondelete='CASCADE'), nullable=False),
)

Group = Table(
    'group',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String(80), nullable=False),
    Column("category", Integer, ForeignKey("category.id", ondelete='CASCADE'), nullable=False),
    Column("source_base", Integer, ForeignKey("source_base.id", ondelete='CASCADE'), nullable=False),
    Column("sheet", String(30), nullable=False, unique=True),
)

Report = Table(
    'report',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String(80), nullable=False),
    Column("category", Integer, ForeignKey("category.id", ondelete='CASCADE'), nullable=False),
    Column("group", Integer, ForeignKey("group.id", ondelete='CASCADE'), nullable=False),
    Column("source_base", Integer, ForeignKey("source_base.id", ondelete='CASCADE'), nullable=False),
    Column("sheet", String(30), nullable=False, unique=True),
)
