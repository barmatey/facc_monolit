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


class SourceRepo(BaseRepo):
    model = Source

    async def create(self, data: entities.SourceCreateData) -> core_types.Id_:
        return await super().create(data.dict())
