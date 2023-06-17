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
        # Get source base
        wire: DataFrame[WireSchema] = await self.wire_repo().retrieve_wire_df(data.source_id)

        # Create group df
        balance = self.group(wire)
        balance.create_group(data.columns)
        balance_group: pd.DataFrame = balance.get_group()

        # Create sheet from group df and save result into database
        sheet_id = await self.sheet_repo().create_from_dataframe(balance_group, drop_index=True, drop_columns=False)
        return sheet_id

    async def create_report(self) -> core_types.Id_:
        pass
