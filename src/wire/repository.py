import math
from abc import ABC, abstractmethod
from loguru import logger
import pandas as pd
import numpy as np
from pandera.typing import DataFrame
from sqlalchemy.sql import select

import finrep

from src import core_types
from .. import db
from .. import helpers
from . import schema
from .models import SourceBase, Wire


class SourceRepo(ABC):
    @abstractmethod
    async def create_source(self, data: schema.CreateSourceForm) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_source(self, id_: core_types.Id_) -> schema.Source:
        pass

    @abstractmethod
    async def delete_source(self, id_: core_types.Id_) -> None:
        pass

    @abstractmethod
    async def retrieve_source_as_dataframe(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass


class SourceRepoPostgres(SourceRepo):

    async def create_source(self, data: schema.CreateSourceForm) -> core_types.Id_:
        async with db.get_async_session() as session:
            insert = SourceBase.insert().values(**data.dict()).returning(SourceBase.c.id)
            result = await session.execute(insert)
            await session.commit()
        return result.fetchone()[0]

    async def retrieve_source(self, id_: core_types.Id_) -> schema.Source:
        async with db.get_async_session() as session:
            q = SourceBase.select().where(SourceBase.c.id == id_)
            source = await session.execute(q)
            source = source.fetchone()

        source = schema.Source(
            id_=source[0],
            title=source[1],
            total_start_date=source[2],
            total_end_date=source[3],
            wcols=source[4],
        )
        return source

    async def delete_source(self, id_: core_types.Id_) -> None:
        async with db.get_async_session() as session:
            delete = SourceBase.delete().where(SourceBase.c.id == id_)
            _ = await session.execute(delete)
            await session.commit()

    async def retrieve_source_as_dataframe(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass


class WireRepo(ABC):
    @abstractmethod
    async def bulk_create_wire(self, wires: list) -> list[core_types.Id_]:
        pass


class WireRepoPostgres(WireRepo):
    async def bulk_create_wire(self, wires: DataFrame[schema.WireSchema]) -> None:
        chunksize = math.ceil(10_000 / len(wires.columns))
        splited = helpers.split_dataframe(wires, chunksize)

        async with db.get_async_session() as session:
            for part in splited:
                insert = Wire.insert().values(part.to_dict(orient='records'))
                await session.execute(insert)
            await session.commit()
