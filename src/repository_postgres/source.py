from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column

from sqlalchemy import String, JSON, TIMESTAMP

from src import core_types
from src.wire import entities
from .base import BaseRepo, BaseModel


def get_wcols():
    return ['sender', 'receiver', 'subconto_first', 'subconto_second', 'comment']


class Source(BaseModel):
    __tablename__ = "source"
    title: Mapped[int] = mapped_column(String(80), nullable=False)
    total_start_date: Mapped[datetime] = mapped_column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    total_end_date: Mapped[datetime] = mapped_column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    wcols: Mapped[list[str]] = mapped_column(JSON, default=get_wcols, nullable=False)

    def to_source_entity(self) -> entities.Source:
        result = entities.Source(
            id=self.id,
            title=self.title,
            total_start_date=self.total_start_date,
            total_end_date=self.total_end_date,
            wcols=self.wcols,
        )
        return result


class SourceRepo(BaseRepo):
    model = Source

    async def create(self, data: entities.SourceCreate) -> entities.Source:
        source_model: Source = await super().create(data.dict())
        return source_model.to_source_entity()

    async def retrieve(self, source_id: core_types.Id_) -> entities.Source:
        filter_ = {"id": source_id}
        source: Source = await super().retrieve(filter_)
        return source.to_source_entity()

    async def retrieve_bulk(self, filter_: dict = None, sort_by: str = None, ascending=True) -> list[entities.Source]:
        if filter_ is None:
            filter_ = {}
        sources: list[Source] = await super().retrieve_bulk(filter_)
        sources: list[entities.Source] = [x.to_source_entity() for x in sources]
        return sources
