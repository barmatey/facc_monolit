import enum
import typing
from abc import ABC, abstractmethod

import pandas as pd

from .. import service_finrep
from . import schema, enums, repository


class FinrepService(ABC):

    @abstractmethod
    async def create_group(self, data: schema.GroupCreateSchema) -> pd.DataFrame:
        pass

    @abstractmethod
    async def create_report(self, data: schema.ReportCreateSchema) -> pd.DataFrame:
        pass


class BaseService(FinrepService, ABC):
    wire_repo: typing.Type[repository.WireRepo] = repository.WireRepo
    group_repo: typing.Type[repository.GroupRepo] = repository.GroupRepo

    interval: typing.Type[service_finrep.Interval] = service_finrep.Interval
    wire: typing.Type[service_finrep.Wire] = service_finrep.Wire

    group: typing.Type[service_finrep.Group]
    report: typing.Type[service_finrep.Report]

    async def create_group(self, data: schema.GroupCreateSchema) -> pd.DataFrame:
        wire_df = await self.wire_repo().retrieve_wire_dataframe(filter_by={"source_id": data.source_id})

        wire = self.wire(wire_df)
        group = self.group()
        await group.create_group(wire, ccols=data.columns)
        group_df = group.get_group_df()
        return group_df

    async def create_report(self, data: schema.ReportCreateSchema) -> pd.DataFrame:
        wire_df = await self.wire_repo().retrieve_wire_dataframe(filter_by={"source_id": data.source_id})
        group_df = await self.group_repo().retrieve_linked_sheet_as_dataframe(group_id=data.group_id)

        wire = self.wire(wire_df)
        group = self.group(group_df)
        interval = self.interval(**data.interval.dict())

        report = self.report()
        await report.create_report(wire, group, interval)
        report = report.get_report()

        return report


class ProfitService(BaseService):
    group = service_finrep.ProfitGroup
    report = service_finrep.ProfitReport


class BalanceService(BaseService):
    group = service_finrep.BalanceGroup
    report = service_finrep.BalanceReport


class LinkedFinrep(enum.Enum):
    BALANCE = BalanceService
    PROFIT = ProfitService
    CASHFLOW = ProfitService


def get_finrep_service(category: enums.CategoryLiteral) -> FinrepService:
    return LinkedFinrep[category].value()
