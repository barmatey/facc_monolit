from abc import ABC, abstractmethod
from loguru import logger
import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy.sql import select

import finrep

from src import core_types
from .. import db
from . import schema
from .models import SourceBase, Wire


class SourceRepo(ABC):
    @abstractmethod
    async def create_source(self, data: schema.CreateSourceForm) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_wire_df(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass


class SourceRepoPostgres(SourceRepo):

    async def create_source(self, data: schema.CreateSourceForm) -> core_types.Id_:
        async with db.get_session() as session:
            insert = SourceBase.insert().values(**data.dict()).returning(SourceBase.c.id)
            result = await session.execute(insert)
            await session.commit()
        return result.fetchone()[0]

    async def retrieve_wire_df(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass


class WireRepo(ABC):
    @abstractmethod
    async def bulk_create_wire(self, source_id: core_types.Id_, wires: list) -> list[core_types.Id_]:
        pass


class WireRepoPostgres(WireRepo):
    async def bulk_create_wire(self, source_id: core_types.Id_, wires: list) -> list[core_types.Id_]:
        async with db.get_session() as session:
            insert = Wire.insert().values(wires).returning(Wire.c.id)
            result = await session.execute(insert)
            await session.commit()
        return result.fetchall()