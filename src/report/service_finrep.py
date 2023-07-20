import enum
import typing
from abc import ABC, abstractmethod

import pandas as pd

from .. import service_finrep
from . import schema, enums


class FinrepService(ABC):

    @abstractmethod
    async def create_group(self, wire_df: pd.DataFrame, target_columns: list[str]) -> pd.DataFrame:
        pass

    @abstractmethod
    async def create_report(self, wire_df: pd.DataFrame, group_df: pd.DataFrame,
                            interval: schema.IntervalCreateSchema) -> pd.DataFrame:
        pass


class BaseService(FinrepService, ABC):
    interval: typing.Type[service_finrep.Interval] = service_finrep.Interval
    wire: typing.Type[service_finrep.Wire] = service_finrep.Wire
    group: typing.Type[service_finrep.Group]
    report: typing.Type[service_finrep.Report]

    async def create_group(self, wire_df: pd.DataFrame, target_columns: list[str]) -> pd.DataFrame:
        wire = self.wire(wire_df)
        group = self.group()
        await group.create_group(wire, ccols=target_columns)
        group_df = group.get_group_df()
        return group_df

    async def create_report(self, wire_df: pd.DataFrame, group_df: pd.DataFrame,
                            interval: schema.IntervalCreateSchema) -> pd.DataFrame:
        wire = self.wire(wire_df)
        group = self.group(group_df)
        interval = self.interval(
            start_date=interval.start_date,
            end_date=interval.end_date,
            total_start_date=interval.total_start_date,
            total_end_date=interval.total_end_date,
            iyear=interval.period_year,
            imonth=interval.period_month,
            iday=interval.period_day,
        )

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
