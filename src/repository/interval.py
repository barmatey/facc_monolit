from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer
from sqlalchemy import TIMESTAMP

from .. import core_types
from ..report import entities
from . import db
from .report import Report
from .base import BaseRepo

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
    Column("report_id", Integer, ForeignKey(Report.c.id, ondelete='CASCADE'), nullable=False),
)


class IntervalRepo(BaseRepo):
    table = Interval

    async def create(self, data: entities.IntervalCreate) -> core_types.Id_:
        async with db.get_async_session() as session:
            return await super()._create(data.dict(), session, commit=True)
