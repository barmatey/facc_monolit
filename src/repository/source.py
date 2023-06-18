from datetime import datetime

from pandera.typing import DataFrame
from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from sqlalchemy import String, JSON, TIMESTAMP

import finrep
from .. import core_types
from ..wire import entities
from . import db
from .base import BaseRepo, BaseModel


def get_wcols():
    return ['sender', 'receiver', 'subconto_first', 'subconto_second', 'comment']


class Source(BaseModel):
    __tablename__ = "source"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[int] = mapped_column(String(80), nullable=False)
    total_start_date: Mapped[datetime] = mapped_column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    total_end_date: Mapped[datetime] = mapped_column(TIMESTAMP, default=datetime.utcnow, nullable=False)
    wcols: Mapped[list[str]] = mapped_column(JSON, default=get_wcols, nullable=False)


class SourceRepo(BaseRepo):
    table = Source

    async def create(self, data: entities.SourceCreateData) -> core_types.Id_:
        return await super().create(data.dict())
