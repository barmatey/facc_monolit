import enum
from abc import ABC, abstractmethod

import pandas as pd

import finrep
from .. import finrep_service
from . import schema, enums, repository


class FinrepService(ABC):

    @abstractmethod
    async def create_group(self, data: schema.GroupCreateSchema) -> pd.DataFrame:
        pass

    @abstractmethod
    async def create_report(self, data: schema.ReportCreateSchema) -> pd.DataFrame:
        pass


class BaseService(FinrepService, ABC):
    wire_repo: repository.WireRepo = repository.WireRepo()
    group_repo: repository.GroupRepo = repository.GroupRepo()


class BalanceService(BaseService):

    async def create_group(self, data: schema.GroupCreateSchema) -> pd.DataFrame:
        wire_df = await self.wire_repo.retrieve_wire_dataframe(filter_by={"source_id": data.source_id})

        balance = finrep.BalanceGroup(wire_df)
        balance.create_group(ccols=data.columns)
        balance_group: pd.DataFrame = balance.get_group()

        return balance_group

    async def create_report(self, data: schema.ReportCreateSchema) -> pd.DataFrame:
        wire_df = await self.wire_repo.retrieve_wire_dataframe(filter_by={"source_id": data.source_id})
        group_df = await self.group_repo.retrieve_linked_sheet_as_dataframe(group_id=data.group_id)

        interval = finrep.BalanceInterval(**data.interval.dict())
        report = finrep.BalanceReport(wire_df, group_df, interval)
        report.create_report()
        report_df = report.get_report()

        return report_df


class ProfitService(BaseService):

    async def create_group(self, data: schema.GroupCreateSchema) -> pd.DataFrame:
        wire_df = await self.wire_repo.retrieve_wire_dataframe(filter_by={"source_id": data.source_id})

        wire = finrep_service.Wire(wire_df)
        group = finrep_service.ProfitGroup()
        await group.create_group(wire, ccols=data.columns)
        group = group.get_group_df()

        return group

    async def create_report(self, data: schema.ReportCreateSchema) -> pd.DataFrame:
        wire_df = await self.wire_repo.retrieve_wire_dataframe(filter_by={"source_id": data.source_id})
        group_df = await self.group_repo.retrieve_linked_sheet_as_dataframe(group_id=data.group_id)

        wire = finrep_service.Wire(wire_df)
        group = finrep_service.ProfitGroup(group_df)
        interval = finrep_service.Interval(**data.interval.dict())

        report = finrep_service.ProfitReport()
        await report.create_report(wire, group, interval)
        report = report.get_report()

        return report


class LinkedFinrep(enum.Enum):
    BALANCE = BalanceService
    PROFIT = ProfitService


def get_finrep_service(category: enums.CategoryLiteral) -> FinrepService:
    return LinkedFinrep[category].value()
