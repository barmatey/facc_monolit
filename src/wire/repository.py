from abc import ABC, abstractmethod
import loguru
import pandas as pd
from pandera.typing import DataFrame
from sqlalchemy.sql import text


import finrep

from src import core_types
from . import schema
from .. import db


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
            q = "SELECT * FROM source_base"
            result = await session.execute(text(q))
            loguru.logger.debug(list(result))
            await session.commit()
        return 12341234

    async def retrieve_wire_df(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass
