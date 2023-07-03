import enum
from abc import ABC, abstractmethod

import pandas as pd

import finrep

from . import entities, enums


class FinrepService(ABC):

    @abstractmethod
    async def create_group(self, wire: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        pass

    @abstractmethod
    async def create_report(self, wire_df: pd.DataFrame, group_df: pd.DataFrame,
                            interval: entities.IntervalCreate) -> pd.DataFrame:
        pass


class BalanceService(FinrepService):

    async def create_group(self, wire: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        balance = finrep.BalanceGroup(wire)
        balance.create_group(ccols=columns)
        balance_group: pd.DataFrame = balance.get_group()
        return balance_group

    async def create_report(self, wire_df: pd.DataFrame, group_df: pd.DataFrame,
                            interval: entities.IntervalCreate) -> pd.DataFrame:
        interval = finrep.BalanceInterval(
            start_date=interval.start_date,
            end_date=interval.end_date,
            iyear=interval.period_year,
            imonth=interval.period_month,
            iday=interval.period_day,
        )
        report = finrep.BalanceReport(wire_df, group_df, interval)
        report.create_report()
        report_df = report.get_report()
        return report_df


class LinkedFinrep(enum.Enum):
    BALANCE = BalanceService


def get_finrep_service(category: enums.CategoryLiteral) -> FinrepService:
    return LinkedFinrep[category].value()
