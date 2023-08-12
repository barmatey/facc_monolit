from abc import ABC, abstractmethod
from typing import Self

import loguru
import numpy as np
import pandas as pd
import pandera

from .wire import Wire
from .group import Group, BalanceGroup
from .interval import Interval


class Report:
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
        wires = self._wire_df.copy()
        wires['interval'] = pd.cut(wires['date'], self._interval.get_intervals(), right=True)
        wires['saldo'] = wires['debit'] - wires['credit']

        needed_cols = ['interval'] + self._ccols + ['saldo']
        wires = (
            wires[needed_cols]
            .dropna(axis=0, how='any')
            .groupby(needed_cols[:-1])
            .sum()
            .reset_index()
        )

        merged_df = pd.merge(wires, self._group.get_group_df(), on=self._ccols, how='inner')
        merged_df.loc[merged_df['reverse'], 'saldo'] = -merged_df.loc[merged_df['reverse'], 'saldo']
        merged_df = merged_df[['saldo', 'interval', ] + self._index_names]

        needed_cols = self._index_names + ['interval']
        report_df = (
            merged_df
            .groupby(needed_cols, observed=True)
            .sum()
            .reset_index()
            .set_index(self._index_names)
        )

        report_df = self._split_df_by_intervals(report_df)

        self._report_df = report_df
        return self

    def sort_by_group(self) -> Self:
        report_df = self._report_df.reset_index()
        if isinstance(self._group, BalanceGroup):
            group_df = (
                self._group.get_splited_group_df()
                .drop_duplicates()
                .reset_index(drop=True)
            )
        else:
            group_df = (
                self._group.get_group_df()
                .drop_duplicates()
                .reset_index(drop=True)
            )

        group_df["__sortcol__"] = range(0, len(group_df.index))
        group_df = group_df[self._index_names + ["__sortcol__"]]

        group_df.loc[:, self._index_names] = group_df.loc[:, self._index_names].astype(str)
        report_df.loc[:, self._index_names] = report_df.loc[:, self._index_names].astype(str)

        report_df = (
            pd.merge(report_df, group_df, on=self._index_names, how='left')
            .sort_values('__sortcol__', ignore_index=True)
            .drop('__sortcol__', axis=1)
        )

        self._report_df = report_df.set_index(self._index_names)
        return self

    def calculate_total(self) -> Self:
        report_df = self._report_df.reset_index().reset_index()
        gcols = self._index_names[:-1]
        sum_cols = self._report_df.columns

        while gcols:
            grouped = report_df.groupby(gcols, as_index=False)
            total: pd.DataFrame = grouped[sum_cols].sum()
            sortcol = grouped['index'].aggregate('min')
            total = pd.merge(total, sortcol, on=gcols)
            report_df = pd.concat([total, report_df], ignore_index=True)
            gcols.pop()

        report_df = (
            report_df
            .sort_values('index')
            .drop('index', axis=1)
            .fillna('TOTAL')
            .set_index(self._index_names)
        )
        self._report_df = report_df
        return self

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
        names = [f"level {i + 1}" for i in range(0, len(gcols) - 1)]
        return names

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


class ProfitReport(Report):
    pass


class BalanceReport(Report):
    def __init__(self, wire: Wire, group: BalanceGroup, interval: Interval):
        super().__init__(wire, group, interval)
        self._agcols = self._find_agcols(self._gcols)
        self._lgcols = self._find_lgcols(self._gcols)
        self._level_gcols = self._create_level_gcols(self._agcols)

    @staticmethod
    def _find_agcols(gcols):
        agcols = [x for x in gcols if 'assets' in x.lower()]
        return agcols

    @staticmethod
    def _find_lgcols(gcols):
        agcols = [x for x in gcols if 'liabs' in x.lower()]
        return agcols

    @staticmethod
    def _create_level_gcols(agcols):
        result = [f"gcol_level_{i}" for i in range(0, len(agcols))]
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

        # Aggregate saldo by interval and ccols
        # Example: ['interval', 'sender', 'subconto', ]
        needed_cols = ['interval'] + self._ccols + ['saldo']
        wires = (
            wires[needed_cols]
            .dropna(axis=0, how='any')
            .groupby(needed_cols[:-1])
            .sum()
            .reset_index()
        )

        # Create report_df from ccols
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

        # Change ccols to gcols and recalculating data in the new version of report_df
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

    def calculate_saldo(self):
        saldo = self._report_df.loc[('assets',), :].sum() - self._report_df.loc[('liabs',), :].sum()
        self._report_df.loc[('saldo',) * len(self._report_df.index.levels), :] = saldo
        return self
