from datetime import datetime

import pandas as pd
from sqlalchemy.orm import Mapped, mapped_column

from sqlalchemy import String, JSON, TIMESTAMP, func

from src.core_types import DTO, OrderBy
from src.wire import entities, repository
from .base import BasePostgres, BaseModel


def get_wcols():
    return [
        {'title': 'date', 'label': 'Date', 'dtype': 'date', },
        {'title': 'sender', 'label': 'Sender', 'dtype': 'float', },
        {'title': 'receiver', 'label': 'Receiver', 'dtype': 'float', },
        {'title': 'debit', 'label': 'Debit', 'dtype': 'float', },
        {'title': 'credit', 'label': 'Credit', 'dtype': 'float', },
        {'title': 'sub1', 'label': 'First subconto', 'dtype': 'str', },
        {'title': 'sub2', 'label': 'Second subconto', 'dtype': 'str', },
        {'title': 'comment', 'label': 'Comment', 'dtype': 'str', },
    ]


# todo i need to delete all linked sheets when i delete source
class SourceModel(BaseModel):
    __tablename__ = "source"
    title: Mapped[int] = mapped_column(String(80), nullable=False)
    total_start_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.utcnow,
                                                       nullable=False)
    total_end_date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), default=datetime.utcnow, nullable=False)
    wcols: Mapped[list[dict]] = mapped_column(JSON, default=get_wcols, nullable=False)
    updated_at: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP(timezone=True), default=func.now(), onupdate=func.now())

    def to_entity(self) -> entities.Source:
        result = entities.Source(
            id=self.id,
            title=self.title,
            total_start_date=pd.Timestamp(self.total_start_date),
            total_end_date=pd.Timestamp(self.total_end_date),
            wcols=list(self.wcols),
            updated_at=pd.Timestamp(self.updated_at),
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

    async def get_uniques(self, columns_by: list[str], filter_by: dict,
                          order_by: OrderBy = None, asc=True, ) -> list[dict]:
        raise NotImplemented

    async def update_one(self, data: DTO, filter_by: dict,) -> entities.Source:
        model: SourceModel = await super().update_one(data, filter_by)
        return model.to_entity()

    async def delete_one(self, filter_by: dict) -> entities.Entity:
        model: SourceModel = await super().delete_one(filter_by)
        return model.to_entity()
