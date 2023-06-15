from datetime import datetime
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Float
from sqlalchemy import String, JSON, TIMESTAMP

metadata = MetaData()


def get_wcols():
    return ['sender', 'receiver', 'subconto_first', 'subconto_second', 'comment']


SourceBase = Table(
    'source_base',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String(80), nullable=False),
    Column("total_start_date", TIMESTAMP, default=datetime.utcnow, nullable=False),
    Column("total_end_date", TIMESTAMP, default=datetime.utcnow, nullable=False),
    Column("wcols", JSON, default=get_wcols, nullable=False),
)

Wire = Table(
    'wire',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("date", TIMESTAMP, nullable=False),
    Column("sender", Float, nullable=False),
    Column("receiver", Float, nullable=False),
    Column("debit", Float, nullable=False),
    Column("credit", Float, nullable=False),
    Column("subconto_first", String(800), nullable=True),
    Column("subconto_second", String(800), nullable=True),
    Column("comment", String(800), nullable=True),
    Column("source_base_id", Integer, ForeignKey(SourceBase.c.id, ondelete='CASCADE'), nullable=False),
)

Category = Table(
    'category',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("value", String(80), nullable=False, unique=True),
)

Group = Table(
    'group',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String(80), nullable=False),
    Column("category", Integer, ForeignKey(Category.c.id, ondelete='CASCADE'), nullable=False),
    Column("source_base", Integer, ForeignKey(SourceBase.c.id, ondelete='CASCADE'), nullable=False),
    Column("sheet", String(30), nullable=False, unique=True),
)

Report = Table(
    'report',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String(80), nullable=False),
    Column("category", Integer, ForeignKey(Category.c.id, ondelete='CASCADE'), nullable=False),
    Column("group", Integer, ForeignKey(Group.c.id, ondelete='CASCADE'), nullable=False),
    Column("source_base", Integer, ForeignKey(SourceBase.c.id, ondelete='CASCADE'), nullable=False),
    Column("sheet", String(30), nullable=False, unique=True),
)
