from abc import ABC, abstractmethod
from typing import Self

import loguru
import numpy as np
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from .wire import Wire
from .group import Group, BalanceGroup
from .interval import Interval

log = loguru.logger.debug


class Report(ABC):
    @abstractmethod
    def create_report_df(self) -> Self:
        raise NotImplemented

    @abstractmethod
    def sort_by_group(self) -> Self:
        raise NotImplemented

    @abstractmethod
    def drop_zero_rows(self) -> Self:
        raise NotImplemented

    @abstractmethod
    def get_report_df(self) -> pd.DataFrame:
        raise NotImplemented


class BaseReportOld(Report):
    def __init__(self, df: pd.DataFrame = None, ccols: list[str] = None, gcols: list[str] = None):
        self._report = df.copy() if df is not None else None
        self._ccols = ccols.copy() if ccols is not None else None
        self._gcols = gcols.copy() if gcols is not None else None

    def drop_zero_rows(self) -> Self:
        self._report = self._report.replace(0, np.nan)
        self._report = self._report.dropna(axis=0, how='all')
        self._report = self._report.replace(np.nan, 0)
        return self

    def _group_wires_by_gcols_and_intervals(self, df: pd.DataFrame, interval: Interval) -> DataFrameGroupBy:
        df["interval"] = pd.cut(df['date'], interval.get_intervals(), right=True)
        grouped_wires = df.groupby(self._gcols + ['interval'], as_index=False)
        return grouped_wires

    def _calculate_saldo_from_grouped_wires(self, grouped_wires: DataFrameGroupBy) -> pd.DataFrame:
        result: pd.DataFrame = grouped_wires[self._gcols + ['debit', 'credit']].sum(numeric_only=True)
        result['saldo'] = result['debit'] - result['credit']
        result = result.drop(['debit', 'credit'], axis=1).set_index(self._gcols)
        return result


class BaseReport(Report):
    def __init__(self, wire: Wire, group: Group, interval: Interval):
        self._wire = wire.copy()
        self._group = group.copy()
        self._interval = interval.copy()

        self._wire_df = wire.get_wire_df().copy()
        self._group_df = group.get_group_df().copy()
        self._ccols = self._find_ccols(self._wire_df.columns, self._group_df.columns)
        self._gcols = self._find_gcols(self._wire_df.columns, self._group_df.columns)
        self._index_names = self._create_index_names(self._gcols)

        self._report_df = None

    def create_report_df(self) -> Self:
        raise NotImplemented

    def sort_by_group(self) -> Self:
        raise NotImplemented

    def drop_zero_rows(self) -> Self:
        self._report_df = self._report_df.replace(0, np.nan)
        self._report_df = self._report_df.dropna(axis=0, how='all')
        self._report_df = self._report_df.replace(np.nan, 0)
        return self

    def get_report_df(self) -> pd.DataFrame:
        if self._report_df is None:
            raise Exception('report is None; Did you forgot create_report_df() function?')
        return self._report_df

    @staticmethod
    def _find_ccols(wire_cols, group_cols):
        ccols = [x for x in group_cols if x in wire_cols]
        if len(ccols) == 0:
            raise Exception("there are no intersections")
        return ccols

    @staticmethod
    def _find_gcols(wire_cols, group_cols):
        gcols = [x for x in group_cols if x not in wire_cols]
        if len(gcols) == 0:
            raise Exception("there are no intersections")
        return gcols

    @staticmethod
    def _create_index_names(gcols):
        names = [f"level {i}" for i in range(0, len(gcols))]
        return names

    @staticmethod
    def _merge_wire_df_with_group_df(wire_df, group_df, gcols, ccols):
        wire_df = wire_df.copy()
        group_df = group_df.copy()

        wire_df.loc[:, ccols] = wire_df.loc[:, ccols].astype(str)
        group_df.loc[:, ccols + gcols] = group_df.loc[:, ccols + gcols].astype(str)

        merged = pd.merge(wire_df, group_df, on=ccols, how='inner')
        return merged


class BalanceReport(BaseReport):
    def __init__(self, wire: Wire, group: Group, interval: Interval):
        super().__init__(wire, group, interval)
        self._agcols = self._find_agcols()
        self._lgcols = self._find_lgcols()

    def _find_agcols(self):
        agcols = [x for x in self._gcols if 'assets' in x.lower()]
        return agcols

    def _find_lgcols(self):
        agcols = [x for x in self._gcols if 'liabs' in x.lower()]
        return agcols

    def create_report_df(self) -> Self:
        merged_wires = self._merge_wire_df_with_group_df(self._wire_df, self._group_df, self._gcols, self._ccols)

        balance_interval = Interval(
            iyear=self._interval.years,
            imonth=self._interval.months,
            iday=self._interval.days,
            start_date=self._wire_df['date'].min() - pd.Timedelta(31, unit='D'),
            end_date=self._interval.end_date,
        )
        merged_wires["interval"] = pd.cut(merged_wires['date'], balance_interval.get_intervals(), right=True)

        assets = self._create_balance_side(merged_wires, gcols=self._agcols)
        liabs = -1 * self._create_balance_side(merged_wires, gcols=self._lgcols)

        report = pd.concat([assets, liabs], keys=['assets', 'liabs'], names=self._index_names).astype(float).round(2)
        report = report.loc[:, report.columns > pd.to_datetime(self._interval.get_start_date()).date()]
        report[report < 0] = 0

        self._report_df = report
        return self

    def sort_by_group(self) -> Self:
        report_df = self._report_df
        group_df = self._group_df
        agcols = self._agcols
        lgcols = self._lgcols
        index_names = self._index_names

        a_group_df = group_df[agcols].drop_duplicates(ignore_index=True).set_index(agcols)
        l_group_df = group_df[lgcols].drop_duplicates(ignore_index=True).set_index(lgcols)
        group_df = pd.concat([a_group_df, l_group_df], keys=['assets', 'liabs']).reset_index()
        group_df.columns = index_names
        group_df['__sortcol__'] = range(0, len(group_df.index))
        report_df = report_df.reset_index()

        group_df.loc[:, index_names] = group_df.loc[:, index_names].astype(str)
        report_df.loc[:, index_names] = report_df.loc[:, index_names].astype(str)

        report_df = (
            pd.merge(report_df, group_df, on=index_names, how='left')
            .sort_values('__sortcol__', ignore_index=True)
            .drop('__sortcol__', axis=1)
        )

        self._report_df = report_df.set_index(index_names)
        return self

    def calculate_saldo(self):
        self._report_df.loc[('saldo',) * len(self._report_df.index.levels), :] = [0] * len(self._report_df.columns)
        return self

    def _create_balance_side(self, df: pd.DataFrame, gcols: list[str]) -> pd.DataFrame:
        group = df.groupby(gcols + ['interval'], as_index=False)
        side: pd.DataFrame = group[gcols + ['debit', 'credit']].sum(numeric_only=True)
        side['saldo'] = side['debit'] - side['credit']
        side = side.drop(['debit', 'credit'], axis=1).set_index(gcols)
        side = self._split_df_by_intervals(side)
        side = side.cumsum(axis=1)
        return side

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
