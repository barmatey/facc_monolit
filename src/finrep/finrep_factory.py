import typing

import pandas as pd

from .wire import Wire
from .interval import Interval
from .group import Group, ProfitGroup, BalanceGroup
from .report import Report, ProfitReport, BalanceReport

CATEGORY = typing.Literal['BALANCE', 'PROFIT', 'CASHFLOW']

LinkedGroup = {
    "BALANCE": BalanceGroup,
    "PROFIT": ProfitGroup,
    "CASHFLOW": ProfitReport,
}

LinkedReport = {
    "BALANCE": BalanceReport,
    "PROFIT": ProfitReport,
    "CASHFLOW": ProfitReport,
}


def get_group(category) -> typing.Type[Group]:
    return LinkedGroup[category]


def get_report(category) -> typing.Type[Report]:
    return LinkedReport[category]


class FinrepFactory:

    def __init__(self, finrep_category: CATEGORY):
        self.__interval = Interval
        self.__wire = Wire
        self.__group = get_group(finrep_category)
        self.__report = get_report(finrep_category)

    def create_wire(self, wire_df: pd.DataFrame) -> Wire:
        return self.__wire(wire_df)

    def create_interval(self,
                        period_year: int,
                        period_month: int,
                        period_day: int,
                        start_date: pd.Timestamp,
                        end_date: pd.Timestamp,
                        total_start_date=None,
                        total_end_date=None) -> Interval:
        return self.__interval(period_year, period_month, period_day, start_date, end_date, total_start_date,
                               total_end_date)

    def create_group_from_wire(self, wire: Wire, ccols: list[str] = None, fixed_ccols: list[str] = None) -> Group:
        return self.__group.from_wire(wire, ccols, fixed_ccols)

    def create_group_from_frame(self, df: pd.DataFrame, ccols: list[str] = None,
                                fixed_ccols: list[str] = None) -> Group:
        return self.__group(df, ccols, fixed_ccols)

    def create_report(self, wire: Wire, group: Group, interval: Interval) -> Report:
        return self.__report(wire, group, interval)
