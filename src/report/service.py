from loguru import logger
import pandas as pd
from abc import ABC, abstractmethod
from pandera.typing import DataFrame

import finrep
from finrep.types import WireSchema

from .. import core_types
from ..repository.group import GroupRepo
from ..repository.report import ReportRepo
from ..repository.sheet import SheetRepo
from ..repository.wire import WireRepo
from .entities import GroupCreate, ReportCreate


class Service(ABC):
    wire_repo = WireRepo
    sheet_repo = SheetRepo
    group_repo = GroupRepo
    report_repo = ReportRepo

    @abstractmethod
    async def create_group(self, data: GroupCreate) -> core_types.Id_:
        pass

    @abstractmethod
    async def create_report(self) -> core_types.Id_:
        pass


class BalanceService(Service):
    group = finrep.BalanceGroup

    async def create_group(self, data: GroupCreate) -> core_types.Id_:
        wire: DataFrame[WireSchema] = await self.wire_repo().retrieve_wire_df(data.source_id)

        balance = self.group(wire)
        balance.create_group(data.columns)

        balance_group: pd.DataFrame = balance.get_group()

        logger.debug(f"\n{balance_group.head(5).to_string()}")
        return 12345678

    async def create_report(self) -> core_types.Id_:
        pass
