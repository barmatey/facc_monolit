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
            obj = data.dict()
            await session.execute(
                SourceBase.insert(), obj
            )
            await session.commit()
            logger.debug(obj)
        return 12341234

    async def retrieve_wire_df(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass
