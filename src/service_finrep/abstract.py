import typing
from abc import ABC, abstractmethod
import pandas as pd

from .wire import Wire
from .interval import Interval
from .group import Group, BalanceGroup, ProfitGroup
from .report import Report, BalanceReport, ProfitReport


class Finrep(ABC):

    @abstractmethod
    def create_group(self, wire_df: pd.DataFrame, target_columns: list[str]) -> Group:
        raise NotImplemented

    @abstractmethod
    def create_interval(self,
                        period_year: int,
                        period_month: int,
                        period_day: int,
                        start_date: pd.Timestamp,
                        end_date: pd.Timestamp,
                        total_start_date=None,
                        total_end_date=None):
        raise NotImplemented

    @abstractmethod
    def create_report(self, wire_df: pd.DataFrame, group_df: pd.DataFrame, interval: Interval) -> pd.DataFrame:
        raise NotImplemented


class BaseFinrep(Finrep, ABC):
    interval = Interval
    wire = Wire
    group: typing.Type[Group] = NotImplemented
    report: typing.Type[Report] = NotImplemented

    def create_group(self, wire_df: pd.DataFrame, target_columns: list[str]) -> Group:
        wire = self.wire(wire_df)
        group = self.group()
        group.create_group(wire, ccols=target_columns)
        return group

    def create_interval(self,
                        period_year: int,
                        period_month: int,
                        period_day: int,
                        start_date: pd.Timestamp,
                        end_date: pd.Timestamp,
                        total_start_date=None,
                        total_end_date=None):
        return self.interval(period_year, period_month, period_day, start_date, end_date, total_start_date,
                             total_end_date)

    def create_report(self, wire_df: pd.DataFrame, group_df: pd.DataFrame, interval: Interval) -> pd.DataFrame:
        wire = self.wire(wire_df)
        group = self.group(group_df)

        report = self.report()
        report.create_report(wire, group, interval)
        report = report.get_report()

        return report


class ProfitFinrep(BaseFinrep):
    group = ProfitGroup
    report = ProfitReport


class BalanceFinrep(BaseFinrep):
    group = BalanceGroup
    report = BalanceReport
