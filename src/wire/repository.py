from abc import ABC, abstractmethod

import pandas as pd
from pandera.typing import DataFrame
import finrep

from src import core_types
from . import schema


class SourceRepo(ABC):
    @abstractmethod
    async def create_source(self, data: schema.CreateSourceForm) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_wire_df(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass


class SourceRepoPostgres(SourceRepo):

    async def create_source(self, data: schema.CreateSourceForm) -> core_types.Id_:
        return 12341234

    async def retrieve_wire_df(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass
