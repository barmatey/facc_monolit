from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer
from sqlalchemy import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime

import core_types
from report import entities
from . import db
from .base import BaseRepo, BaseModel


class Interval(BaseModel):
    __tablename__ = 'interval'
    total_start_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    total_end_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    start_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    end_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    period_year: Mapped[int] = mapped_column(Integer, nullable=False)
    period_month: Mapped[int] = mapped_column(Integer, nullable=False)
    period_day: Mapped[int] = mapped_column(Integer, nullable=False)


class IntervalRepo(BaseRepo):
    model = Interval

    async def create(self, data: entities.ReportIntervalCreate) -> core_types.Id_:
        return await super().create(data.dict())
