from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Mapped, mapped_column

from sqlalchemy import String, JSON, TIMESTAMP

import core_types
from core_types import DTO, OrderBy
from src.wire import entities, repository
from .base import BasePostgres, BaseModel


def get_wcols():
    return [
        {'title': 'date', 'label': 'Date', 'dtype': 'date', },
        {'title': 'sender', 'label': 'Sender', 'dtype': 'float', },
        {'title': 'receiver', 'label': 'Receiver', 'dtype': 'float', },
        {'title': 'debit', 'label': 'Debit', 'dtype': 'float', },
        {'title': 'credit', 'label': 'Credit', 'dtype': 'float', },
        {'title': 'subconto_first', 'label': 'First subconto', 'dtype': 'str', },
        {'title': 'subconto_second', 'label': 'Second subconto', 'dtype': 'str', },
        {'title': 'comment', 'label': 'Comment', 'dtype': 'str', },
    ]


# todo i need to delete all linked sheets when i delete source
class SourceModel(BaseModel):
    __tablename__ = "source"
    title: Mapped[int] = mapped_column(String(80), nullable=False)
    total_start_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.utcnow,
                                                       nullable=False)
    total_end_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.utcnow, nullable=False)
    wcols: Mapped[list[str]] = mapped_column(JSON, default=get_wcols, nullable=False)

    def to_entity(self) -> entities.Source:
        result = entities.Source(
            id=self.id,
            title=self.title,
            total_start_date=self.total_start_date,
            total_end_date=self.total_end_date,
            wcols=self.wcols,
        )
        return result


class SourceRepoPostgres(BasePostgres, repository.RepositoryCrud):
    model = SourceModel

    async def create_one(self, data: DTO) -> entities.Source:
        model: SourceModel = await super().create_one(data)
        return model.to_entity()

    async def get_one(self, filter_by: dict) -> entities.Source:
        model: SourceModel = await super().get_one(filter_by)
        return model.to_entity()

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True, slice_from: int = None,
                       slice_to: int = None) -> list[entities.Source]:
        models: list[SourceModel] = await super().get_many(filter_by, order_by, asc, slice_from, slice_to)
        sources = [x.to_entity() for x in models]
        return sources

    async def update_one(self, filter_by: dict, data: DTO) -> entities.Source:
        model: SourceModel = await super().update_one(data, filter_by)
        return model.to_entity()

    async def delete_one(self, filter_by: dict) -> core_types.Id_:
        model: SourceModel = await super().delete_one(filter_by)
        return model.id
