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
        async with db.get_async_session() as session:
            return await super()._create(data.dict(), session, commit=True)

    async def retrieve(self, id_: core_types.Id_) -> entities.Source:
        async with db.get_async_session() as session:
            data = await super()._retrieve(id_, session)
            source = entities.Source(**data)
            return source

    async def delete(self, id_: core_types.Id_) -> None:
        async with db.get_async_session() as session:
            await super()._delete(id_, session, commit=True)

    async def retrieve_source_as_dataframe(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass
