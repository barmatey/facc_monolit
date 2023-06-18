from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer
from sqlalchemy import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime

from .. import core_types
from ..report import entities
from . import db
from .base import BaseRepo, BaseModel


class Interval(BaseModel):
    __tablename__ = 'interval'
    id: Mapped[int] = mapped_column(primary_key=True)
    total_start_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    total_end_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    start_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    end_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    period_year: Mapped[int] = mapped_column(Integer, nullable=False)
    period_month: Mapped[int] = mapped_column(Integer, nullable=False)
    period_day: Mapped[int] = mapped_column(Integer, nullable=False)


class IntervalRepo(BaseRepo):
    table = Interval

    async def create(self, data: entities.ReportIntervalCreate) -> core_types.Id_:
        async with db.get_async_session() as session:
            return await super()._create(data.dict(), session, commit=True)
