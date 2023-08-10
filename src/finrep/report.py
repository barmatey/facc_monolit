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


class BaseReportOld:
    def __init__(self, df: pd.DataFrame = None, ccols: list[str] = None, gcols: list[str] = None):
        self._report = df.copy() if df is not None else None
        self._ccols = ccols.copy() if ccols is not None else None
        self._gcols = gcols.copy() if gcols is not None else None

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

    @staticmethod
    def _split_df_by_intervals(df: pd.DataFrame) -> pd.DataFrame:
        if 'interval' not in df.columns:
            raise ValueError('"interval" not in df.columns')
        if len(df.columns) > 2:
            raise ValueError(f'function expected df with to columns only (and the first column must be "interval")'
                             f'real columns are: {df.columns}')

        splited = []
        columns = []

        for interval in df['interval'].unique():
            series = df.loc[df['interval'] == interval].drop('interval', axis=1)
            splited.append(series)
            columns.append(interval.right.date())
        splited = pd.concat(splited, axis=1).fillna(0)
        splited.columns = columns
        return splited


class BalanceReport(BaseReport):
    def __init__(self, wire: Wire, group: BalanceGroup, interval: Interval):
        super().__init__(wire, group, interval)
        self._agcols = self._find_agcols()
        self._lgcols = self._find_lgcols()
        self._level_gcols = self._create_level_gcols()

    def _find_agcols(self):
        agcols = [x for x in self._gcols if 'assets' in x.lower()]
        return agcols

    def _find_lgcols(self):
        agcols = [x for x in self._gcols if 'liabs' in x.lower()]
        return agcols

    def _create_level_gcols(self):
        result = [f"gcol_level_{i}" for i in range(0, len(self._agcols))]
        return result

    @staticmethod
    def _create_index_names(gcols):
        names = [f"level {i}" for i in range(0, int(len(gcols) / 2) + 1)]
        return names

    def create_report_df(self) -> Self:
        balance_interval = Interval(
            iyear=self._interval.years,
            imonth=self._interval.months,
            iday=self._interval.days,
            start_date=self._wire_df['date'].min() - pd.Timedelta(31, unit='D'),
            end_date=self._interval.end_date,
        )
        wires = self._wire_df.copy()
        wires['interval'] = pd.cut(wires['date'], balance_interval.get_intervals(), right=True)
        wires['saldo'] = wires['debit'] - wires['credit']

        # Aggregate saldo by interval column and ccols
        # Example: ['interval', 'sender', 'subconto', ]
        needed_cols = ['interval'] + self._ccols + ['saldo']
        wires = (
            wires[needed_cols]
            .dropna(axis=0, how='any')
            .groupby(needed_cols[:-1])
            .sum()
            .reset_index()
        )

        # Merging wire_df and group_df
        merged_df = pd.merge(wires, self._group.get_group_df(), on=self._ccols, how='inner')

        common = ['saldo', 'interval']
        columns = common + self._level_gcols

        assets = merged_df.loc[merged_df['saldo'] >= 0][common + self._agcols]
        assets.columns = columns
        assets = assets.groupby(['interval'] + self._level_gcols).sum().reset_index().set_index(self._level_gcols)

        liabs = merged_df.loc[merged_df['saldo'] < 0][common + self._lgcols]
        liabs.columns = columns
        liabs = liabs.groupby(['interval'] + self._level_gcols).sum().reset_index().set_index(self._level_gcols)

        report_df = pd.concat([assets, liabs], keys=["assets", "liabs"])
        report_df = self._split_df_by_intervals(report_df)
        report_df = (
            report_df
            .reset_index()
            .drop('level_0', axis=1)
            .groupby(self._level_gcols)
            .sum()
            .cumsum(axis=1)
        )
        assets = report_df.copy()
        for col in assets.columns:
            assets[col] = np.where(assets[col] <= 0, 0, assets[col])

        liabs = report_df.copy()
        for col in liabs.columns:
            liabs[col] = np.where(liabs[col] > 0, 0, -liabs[col])

        report_df = pd.concat([assets, liabs], keys=['assets', 'liabs'], names=self._index_names)
        report_df = report_df.loc[:, report_df.columns > pd.to_datetime(self._interval.get_start_date()).date()]

        self._report_df = report_df.round(2)
        return self

    def sort_by_group(self) -> Self:
        report_df = self._report_df.reset_index()
        group_df = (
            self._group.get_splited_group_df()
            .drop_duplicates()
            .reset_index(drop=True)
        )
        group_df["__sortcol__"] = range(0, len(group_df.index))

        group_df.loc[:, self._index_names] = group_df.loc[:, self._index_names].astype(str)
        report_df.loc[:, self._index_names] = report_df.loc[:, self._index_names].astype(str)

        report_df = (
            pd.merge(report_df, group_df, on=self._index_names, how='left')
            .sort_values('__sortcol__', ignore_index=True)
            .drop('__sortcol__', axis=1)
        )

        self._report_df = report_df.set_index(self._index_names)
        return self

    def calculate_saldo(self):
        saldo = self._report_df.loc[('assets',), :].sum() - self._report_df.loc[('liabs',), :].sum()
        self._report_df.loc[('saldo',) * len(self._report_df.index.levels), :] = saldo
        return self

    def _create_balance_side(self, df: pd.DataFrame, gcols: list[str]) -> pd.DataFrame:
        group = df.groupby(gcols + ['interval'], as_index=False)
        side: pd.DataFrame = group[gcols + ['debit', 'credit']].sum(numeric_only=True)
        side['saldo'] = side['debit'] - side['credit']
        side = side.drop(['debit', 'credit'], axis=1).set_index(gcols)
        side = self._split_df_by_intervals(side)
        side = side.cumsum(axis=1)
        return side
