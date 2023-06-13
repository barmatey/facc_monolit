from abc import ABC, abstractmethod

import pandas as pd
from pandera.typing import DataFrame
import finrep

from src import core_types


class WireRepo(ABC):
    @abstractmethod
    async def retrieve_wire_df(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass


class WireRepoPostgres(WireRepo):
    async def retrieve_wire_df(self, wire_base_id: core_types.Id_) -> DataFrame[finrep.typing.WireSchema]:
        pass
