from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer
from sqlalchemy import TIMESTAMP

from .. import models, core_types
from . import db
from .report import Report

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


class IntervalRepo:
    table = Interval

    async def create(self, data: models.Interval, session=None, commit=False, close=False) -> core_types.Id_:
        async with db.get_async_session() as session:
            insert = self.table.insert().values(**data.dict()).returning(self.table.c.id)
            result = await session.execute(insert)
            await session.commit()
            result = result.fetchone()[0]
            return result
