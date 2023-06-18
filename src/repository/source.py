from datetime import datetime

from pandera.typing import DataFrame
from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy import String, JSON, TIMESTAMP

import finrep
from .. import core_types
from ..wire import entities
from . import db
from .base import BaseRepo

metadata = MetaData()


def get_wcols():
    return ['sender', 'receiver', 'subconto_first', 'subconto_second', 'comment']


SourceBase = Table(
    'source',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String(80), nullable=False),
    Column("total_start_date", TIMESTAMP, default=datetime.utcnow, nullable=False),
    Column("total_end_date", TIMESTAMP, default=datetime.utcnow, nullable=False),
    Column("wcols", JSON, default=get_wcols, nullable=False),
)


class SourceRepo(BaseRepo):
    table = SourceBase

    async def create(self, data: entities.SourceCreateData) -> core_types.Id_:
        return await super().create(data.dict())
