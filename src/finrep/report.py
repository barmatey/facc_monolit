from abc import ABC, abstractmethod
from typing import Self

import loguru
import numpy as np
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from .wire import Wire
from .group import Group, BalanceGroup
from .interval import Interval


class Report(ABC):
    @abstractmethod
    def create_report_df(self, wire: Wire, group: Group, interval: Interval) -> Self:
        raise NotImplemented

    @abstractmethod
    def sort_by_group(self, group: Group) -> Self:
        raise NotImplemented

    @abstractmethod
    def drop_zero_rows(self) -> Self:
        raise NotImplemented

    @abstractmethod
    def get_report_df(self) -> pd.DataFrame:
        raise NotImplemented


class BaseReport(Report):
    def __init__(self, df: pd.DataFrame = None, ccols: list[str] = None, gcols: list[str] = None):
        self._report = df.copy() if df is not None else None
        self._ccols = ccols.copy() if ccols is not None else None
        self._gcols = gcols.copy() if gcols is not None else None

    def create_report_df(self, wire: Wire, group: Group, interval: Interval) -> Self:
        raise NotImplemented

    def drop_zero_rows(self) -> Self:
        self._report = self._report.replace(0, np.nan)
        self._report = self._report.dropna(axis=0, how='all')
        self._report = self._report.replace(np.nan, 0)
        return self

    def get_report_df(self) -> pd.DataFrame:
        if self._report is None:
            raise Exception('report is None; Did you forgot create_report_df() function?')
        return self._report

    def _find_ccols(self, wire_cols: list[str], group_cols: list[str]):
        ccols = [x for x in group_cols if x in wire_cols]
        if len(ccols) == 0:
            raise Exception("there are no intersections")
        self._ccols = ccols

    def _find_gcols(self, wire_cols: list[str], group_cols: list[str]):
        gcols = [x for x in group_cols if x not in wire_cols]
        if len(gcols) == 0:
            raise Exception("there are no intersections")
        self._gcols = gcols

    @staticmethod
    def _split_df_by_intervals(df: pd.DataFrame) -> pd.DataFrame:
        if 'interval' not in df.columns:
            raise ValueError('"interval" not in df.columns')
        if len(df.columns) > 2:
            raise ValueError('function expected df with to columns only (and the first column must be "interval")')

        splited = []
        columns = []

        for interval in df['interval'].unique():
            series = df.loc[df['interval'] == interval].drop('interval', axis=1)
            splited.append(series)
            columns.append(interval.right.date())

        splited = pd.concat(splited, axis=1)
        splited.columns = columns
        return splited

    def _merge_wire_df_with_group_df(self, wire: pd.DataFrame, group: pd.DataFrame):
        ccols = self._ccols
        gcols = self._gcols
        wire = wire.copy()
        group = group.copy()

        wire.loc[:, ccols] = wire.loc[:, ccols].astype(str)
        group.loc[:, ccols + gcols] = group.loc[:, ccols + gcols].astype(str)

        merged = pd.merge(wire, group, on=ccols, how='inner')
        return merged

    def _group_wires_by_gcols_and_intervals(self, df: pd.DataFrame, interval: Interval) -> DataFrameGroupBy:
        df["interval"] = pd.cut(df['date'], interval.get_intervals(), right=True)
        grouped_wires = df.groupby(self._gcols + ['interval'], as_index=False)
        return grouped_wires

    def _calculate_saldo_from_grouped_wires(self, grouped_wires: DataFrameGroupBy) -> pd.DataFrame:
        result: pd.DataFrame = grouped_wires[self._gcols + ['debit', 'credit']].sum(numeric_only=True)
        result['saldo'] = result['debit'] - result['credit']
        result = result.drop(['debit', 'credit'], axis=1).set_index(self._gcols)
        return result

    def _sort_by_group(self, report_df: pd.DataFrame, group_df: pd.DataFrame) -> pd.DataFrame:
        gcols = self._gcols
        group_df = group_df[gcols].drop_duplicates(ignore_index=True).reset_index().set_index(gcols)
        report_df = (
            pd.merge(report_df, group_df, left_index=True, right_index=True, how='left', validate='one_to_one')
            .sort_values('index')
            .drop('index', axis=1)
        )
        return report_df


class BalanceReport(BaseReport):

    def __init__(self, df: pd.DataFrame = None, ccols: list[str] = None, gcols: list[str] = None):
        super().__init__(df, ccols, gcols)
        self._agcols = None
        self._lgcols = None

    def create_report_df(self, wire: Wire, group: BalanceGroup, interval: Interval) -> Self:
        wire_df = wire.get_wire_df()
        group_df = group.get_group_df()

        self._find_ccols(wire_df.columns, group_df.columns)
        self._find_gcols(wire_df.columns, group_df.columns)

        merged_wires = self._merge_wire_df_with_group_df(wire_df, group_df)
        merged_wires["interval"] = pd.cut(merged_wires['date'], interval.get_intervals(), right=True)

        assets = self._create_balance_side(merged_wires, gcols=self._agcols)
        liabs = -1 * self._create_balance_side(merged_wires, gcols=self._lgcols)

        report = pd.concat([assets, liabs], keys=['assets', 'liabs'])
        report[report < 0] = 0
        self._report = report
        return self

    def sort_by_group(self, group: Group) -> Self:
        group_df = group.get_group_df()
        agcols = self._agcols
        lgcols = self._lgcols

        a_group_df = group_df[agcols].drop_duplicates(ignore_index=True).set_index(agcols)
        l_group_df = group_df[lgcols].drop_duplicates(ignore_index=True).set_index(lgcols)
        group_df = pd.concat([a_group_df, l_group_df], keys=['assets', 'liabs'])
        group_df['__sortcol__'] = range(0, len(group_df.index))

        loguru.logger.debug(f'\nGROUP_DF:'
                            f'\n{group_df}\n\n')

        self._report = (
            pd.merge(self._report, group_df, left_index=True, right_index=True, how='left', validate='one_to_one')
            .sort_values('__sortcol__')
            # .drop('__sortcol__', axis=1)
        )
        loguru.logger.debug(f'\nSORTED_REPORT:'
                            f'\n{self._report["__sortcol__"]}')
        return self

    def calculate_saldo(self) -> Self:
        self._report.loc[('saldo',) * len(self._report.index.levels), :] = [0] * len(self._report.columns)
        return self

    def _find_gcols(self, wire_cols: list[str], group_cols: list[str]):
        self._gcols = [x for x in group_cols if x not in wire_cols]
        if len(self._gcols) == 0:
            raise Exception("there are no intersections")
        self._agcols = [x for x in self._gcols if 'assets' in x.lower()]
        self._lgcols = [x for x in self._gcols if 'liabs' in x.lower()]

    def _create_balance_side(self, df: pd.DataFrame, gcols: list[str]) -> pd.DataFrame:
        group = df.groupby(gcols + ['interval'], as_index=False)
        side: pd.DataFrame = group[gcols + ['debit', 'credit']].sum(numeric_only=True)
        side['saldo'] = side['debit'] - side['credit']
        side = side.drop(['debit', 'credit'], axis=1).set_index(gcols)
        side = super()._split_df_by_intervals(side)
        side = side.cumsum(axis=1)
        return side
