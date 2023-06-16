from datetime import datetime

from pandera.typing import DataFrame
from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy import String, JSON, TIMESTAMP

import finrep
from .. import models, core_types
from . import db

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


class SourceRepo:
    table = SourceBase

    async def create_source(self, data: models.SourceCreateData) -> core_types.Id_:
        async with db.get_async_session() as session:
            insert = self.table.insert().values(**data.dict()).returning(SourceBase.c.id)
            result = await session.execute(insert)
            await session.commit()
        return result.fetchone()[0]

    async def retrieve_source(self, id_: core_types.Id_) -> models.Source:
        async with db.get_async_session() as session:
            q = self.table.select().where(SourceBase.c.id == id_)
            source = await session.execute(q)
            source = source.fetchone()

            source = models.Source(
                id=source[0],
                title=source[1],
                total_start_date=source[2],
                total_end_date=source[3],
                wcols=source[4],
            )
            return source

    async def delete_source(self, id_: core_types.Id_) -> None:
        async with db.get_async_session() as session:
            delete = self.table.delete().where(SourceBase.c.id == id_)
            _ = await session.execute(delete)
            await session.commit()

    async def retrieve_source_as_dataframe(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass
