from loguru import logger
import pandas as pd
from abc import ABC, abstractmethod
from pandera.typing import DataFrame

import finrep
from finrep.types import WireSchema

from .. import core_types
from ..repository.group import GroupRepo
from ..repository.report import ReportRepo
from ..repository.wire import WireRepo
from ..repository.sheet import SheetRepo
from . import entities, schema, enums


class Service(ABC):

    @abstractmethod
    async def create_group(self, data: schema.GroupCreateSchema) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_group(self, id_: core_types.Id_) -> entities.GroupRetrieve:
        pass

    @abstractmethod
    async def delete_group(self, id_: core_types.Id_) -> core_types.Id_:
        pass

    @abstractmethod
    async def create_report(self, data: schema.ReportCreateSchema) -> core_types.Id_:
        pass


class BaseService(Service):
    wire_repo = WireRepo
    group_repo = GroupRepo
    report_repo = ReportRepo

    async def create_group(self, data: entities.GroupCreate) -> core_types.Id_:
        raise NotImplemented

    async def create_report(self, data: schema.ReportCreateSchema) -> core_types.Id_:
        raise NotImplemented

    async def retrieve_group(self, id_: core_types.Id_) -> entities.GroupRetrieve:
        group: entities.GroupRetrieve = await self.group_repo().retrieve_by_id(id_)
        return group

    async def delete_group(self, id_: core_types.Id_) -> core_types.Id_:
        deleted_id = await self.group_repo().delete_by_id(id_)
        return deleted_id


class BalanceService(BaseService):
    group = finrep.BalanceGroup
    report = finrep.BalanceReport
    interval = finrep.BalanceInterval

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
            category=enums.Category.BALANCE,
        )
        group_id = await self.group_repo().create(group_create_data)
        return group_id

    async def create_report(self, data: schema.ReportCreateSchema) -> core_types.Id_:
        # Get source base
        wire: DataFrame[WireSchema] = await self.wire_repo().retrieve_wire_df(data.source_id)

        # Get group df
        group_df: pd.DataFrame = await self.group_repo().retrieve_linked_sheet_as_dataframe(data.group_id)

        # Create report df
        interval = self.interval(**data.interval.dict())

        # Create report model

        return 321
