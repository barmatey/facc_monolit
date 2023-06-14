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
    async def retrieve_wire_df(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass


class SourceRepoPostgres(SourceRepo):

    async def create_source(self, data: schema.CreateSourceForm) -> core_types.Id_:
        async with db.get_async_session() as session:
            insert = SourceBase.insert().values(**data.dict()).returning(SourceBase.c.id)
            result = await session.execute(insert)
            await session.commit()
        return result.fetchone()[0]

    async def retrieve_wire_df(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
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
