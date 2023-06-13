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
    Column("date", TIMESTAMP),
    Column("sender", Float),
    Column("receiver", Float),
    Column("debit", Float),
    Column("credit", Float),
    Column("subconto_first", String(80)),
    Column("subconto_second", String(80)),
    Column("comment", String(80)),
    Column("source_base_id", Integer, ForeignKey("source_base.id", ondelete='CASCADE')),
)
