from datetime import datetime
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Float
from sqlalchemy import String, JSON, TIMESTAMP


metadata = MetaData()

SourceBase = Table(
    'source_base',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("title", String(80)),
    Column("total_start_date", TIMESTAMP, default=datetime.utcnow),
    Column("total_end_date", TIMESTAMP, default=datetime.utcnow),
    Column("wcols", JSON, default=['sender', 'receiver', 'subconto_first', 'subconto_second', 'comment']),
)

Wire = Table(
    'wire',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("date", TIMESTAMP, nullable=False),
    Column("sender", Float, nullable=False),
    Column("receiver", Float, nullable=False),
    Column("debit", Float, nullable=False),
    Column("credit", Float, nullable=False),
    Column("subconto_first", String(80), nullable=True),
    Column("subconto_second", String(80), nullable=True),
    Column("comment", String(80), nullable=True),
    Column("source_base_id", Integer, ForeignKey("source_base.id", ondelete='CASCADE'), nullable=False),
)
