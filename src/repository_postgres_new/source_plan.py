from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Mapped, mapped_column

from sqlalchemy import String, JSON, TIMESTAMP, func, Float, Integer, ForeignKey

from src import core_types
from src.core_types import DTO, OrderBy
from src.wire import entities, repository
from .base import BaseEntityPostgres, BaseModel

from .source import SourceModel


class PlanItemModel(BaseModel):
    __tablename__ = "plan_item"
    sender = mapped_column(Float, nullable=False)
    receiver = mapped_column(Float, nullable=False)
    sub1 = mapped_column(String(800), nullable=True)
    sub2 = mapped_column(String(800), nullable=True)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey(SourceModel.id, ondelete='CASCADE'), nullable=False)

    updated_at: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP(timezone=True), default=func.now(), onupdate=func.now())

    def to_entity(self, **kwargs) -> entities.PlanItem:
        return entities.PlanItem(
            sender=self.sender,
            receiver=self.receiver,
            sub1=self.sub1,
            sub2=self.sub2,
            source_id=self.source_id,
        )


class PlanItemPostgres(BaseEntityPostgres, repository.RepositoryCrud):
    model = PlanItemModel
