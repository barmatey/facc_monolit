from datetime import datetime
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Float
from sqlalchemy import String, JSON, TIMESTAMP
from .. import models

metadata = MetaData()

Interval = Table(
    'interval',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("total_start_date", TIMESTAMP(timezone=True), nullable=True),
    Column("total_end_date", TIMESTAMP(timezone=True), nullable=True),
    Column("start_date", TIMESTAMP(timezone=True), nullable=False),
    Column("end_date", TIMESTAMP(timezone=True), nullable=False),
    Column("period_year", Integer, nullable=False),
    Column("period_month", Integer, nullable=False),
    Column("period_day", Integer, nullable=False),
    Column("report_id", Integer, ForeignKey(models.Report.c.id, ondelete='CASCADE'), nullable=False),
)
