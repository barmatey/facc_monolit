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
from . import entities, schema


class Service(ABC):
    wire_repo = WireRepo
    group_repo = GroupRepo
    report_repo = ReportRepo

    @abstractmethod
    async def create_group(self, data: schema.GroupCreateSchema) -> core_types.Id_:
        pass

    @abstractmethod
    async def create_report(self) -> core_types.Id_:
        pass


class BalanceService(Service):
    group = finrep.BalanceGroup

    async def create_group(self, data: schema.GroupCreateSchema) -> core_types.Id_:
        # Get source base
        wire: DataFrame[WireSchema] = await self.wire_repo().retrieve_wire_df(data.source_id)

        # Create group df
        balance = self.group(wire)
        balance.create_group(data.columns)
        balance_group: pd.DataFrame = balance.get_group()

        # Create group model
        group_create_data = entities.GroupCreate(
            title=data.title,
            source_id=data.source_id,
            columns=data.columns,
            dataframe=balance_group,
            drop_index=True,
            drop_columns=False,
            category='BALANCE'
        )
        group_id = await self.group_repo().create(group_create_data)
        return group_id

    async def create_report(self) -> core_types.Id_:
        pass
