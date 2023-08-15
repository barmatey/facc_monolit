from sqlalchemy import Integer
from sqlalchemy import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime

from src.rep import entities
from .base import BasePostgres, BaseModel


class IntervalModel(BaseModel):
    __tablename__ = 'interval'
    total_start_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    total_end_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    start_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    end_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    period_year: Mapped[int] = mapped_column(Integer, nullable=False)
    period_month: Mapped[int] = mapped_column(Integer, nullable=False)
    period_day: Mapped[int] = mapped_column(Integer, nullable=False)

    def to_entity(self) -> entities.Interval:
        return entities.Interval(
            id=self.id,
            total_start_date=self.total_start_date,
            total_end_date=self.total_end_date,
            start_date=self.start_date,
            end_date=self.end_date,
            period_year=self.period_year,
            period_month=self.period_month,
            period_day=self.period_day,
        )


class IntervalRepoPostgres(BasePostgres):
    model = IntervalModel
