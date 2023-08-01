import typing
from abc import ABC, abstractmethod
import pandas as pd

from .wire import Wire
from .interval import Interval
from .group import Group, BalanceGroup, ProfitGroup
from .report import Report, BalanceReport, ProfitReport


class Finrep(ABC):

    @abstractmethod
    def create_group(self, wire_df: pd.DataFrame, target_columns: list[str]) -> pd.DataFrame:
        pass

    @abstractmethod
    def create_report(self, wire_df: pd.DataFrame, group_df: pd.DataFrame, interval: Interval) -> pd.DataFrame:
        pass


class BaseFinrep(Finrep, ABC):
    interval = Interval
    wire = Wire
    group: typing.Type[Group] = NotImplemented
    report: typing.Type[Report] = NotImplemented

    def create_group(self, wire_df: pd.DataFrame, target_columns: list[str]) -> pd.DataFrame:
        wire = self.wire(wire_df)
        group = self.group()
        group.create_group(wire, ccols=target_columns)
        group_df = group.get_group_df()
        return group_df

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
