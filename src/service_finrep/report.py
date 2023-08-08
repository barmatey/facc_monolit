import loguru
from loguru import logger
import numpy as np
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from .wire import Wire
from .group import Group, ProfitGroup, BalanceGroup
from .interval import Interval


class Report:
    def __init__(self, df: pd.DataFrame = None):
        self.report = df.copy() if df is not None else None
        self._ccols = None
        self._gcols = None
        self._agcols = None
        self._lgcols = None

    def create_report(self, wire: Wire, group: Group, interval: Interval) -> None:
        raise NotImplemented

    def get_report(self) -> pd.DataFrame:
        if self.report is None:
            raise Exception('report is None; Did you forgot create_report() function?')
        return self.report

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

    @staticmethod
    def _drop_zero_rows(df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace(0, np.nan)
        df = df.dropna(axis=0, how='all')
        df = df.replace(np.nan, 0)
        return df

    def _sort_by_group(self, report_df: pd.DataFrame, group_df: pd.DataFrame) -> pd.DataFrame:
        gcols = self._gcols
        group_df = group_df[gcols].drop_duplicates(ignore_index=True).reset_index().set_index(gcols)
        report_df = (
            pd.merge(report_df, group_df, left_index=True, right_index=True, how='left', validate='one_to_one')
            .sort_values('index')
            .drop('index', axis=1)
        )
        return report_df


class ProfitReport(Report):
    def _find_gcols(self, wire_cols: list[str], group_cols: list[str]):
        gcols = [x for x in group_cols if x not in wire_cols]
        if len(gcols) == 0:
            raise Exception("there are no intersections")
        gcols.pop(gcols.index('reverse'))
        self._gcols = gcols

    def create_report(self, wire: Wire, group: ProfitGroup, interval: Interval) -> None:
        wire_df = wire.get_wire_df()
        group_df = group.get_group_df()

        super()._find_ccols(wire_df.columns, group_df.columns)
        self._find_gcols(wire_df.columns, group_df.columns)

        merged_wires = super()._merge_wire_df_with_group_df(wire_df, group_df)
        merged_wires.loc[merged_wires['reverse'], 'debit'] = -1 * merged_wires.loc[merged_wires['reverse'], 'debit']
        merged_wires.loc[merged_wires['reverse'], 'credit'] = -1 * merged_wires.loc[merged_wires['reverse'], 'credit']
        merged_wires = merged_wires.drop('reverse', axis=1)

        grouped_wires = super()._group_wires_by_gcols_and_intervals(merged_wires, interval)

        report = super()._calculate_saldo_from_grouped_wires(grouped_wires)
        report = super()._split_df_by_intervals(report)
        report = super()._drop_zero_rows(report)
        report = super()._sort_by_group(report, group_df)

        report = report.round(2)
        self.report = report


class BalanceReport(Report):
    def __init__(self, df: pd.DataFrame = None):
        super().__init__(df)
        self._agcols = None
        self._lgcols = None

    def _find_gcols(self, wire_cols: list[str], group_cols: list[str]):
        self._gcols = [x for x in group_cols if x not in wire_cols]
        if len(self._gcols) == 0:
            raise Exception("there are no intersections")

        self._agcols = [x for x in self._gcols if 'assets' in x.lower()]
        self._lgcols = [x for x in self._gcols if 'liabs' in x.lower()]

    def create_report(self, wire: Wire, group: BalanceGroup, interval: Interval) -> None:
        wire_df = wire.get_wire_df()
        group_df = group.get_group_df()

        super()._find_ccols(wire_df.columns, group_df.columns)
        self._find_gcols(wire_df.columns, group_df.columns)

        merged_wires = super()._merge_wire_df_with_group_df(wire_df, group_df)
        merged_wires["interval"] = pd.cut(merged_wires['date'], interval.get_intervals(), right=True)

        assets = self._create_balance_side(merged_wires, gcols=self._agcols)
        liabs = -1 * self._create_balance_side(merged_wires, gcols=self._lgcols)

        report = pd.concat([assets, liabs], keys=['assets', 'liabs'])
        report[report < 0] = 0
        report = super()._drop_zero_rows(report)

        report = self._sort_by_group(report, group_df)

        saldo = assets.sum() + liabs.sum()
        report.loc[('saldo',) * len(report.index.levels), :] = saldo.tolist()

        report = report.round(2)
        self.report = report

    def _create_balance_side(self, df: pd.DataFrame, gcols: list[str]) -> pd.DataFrame:
        group = df.groupby(gcols + ['interval'], as_index=False)
        side: pd.DataFrame = group[gcols + ['debit', 'credit']].sum(numeric_only=True)
        side['saldo'] = side['debit'] - side['credit']
        side = side.drop(['debit', 'credit'], axis=1).set_index(gcols)
        side = super()._split_df_by_intervals(side)
        side = side.cumsum(axis=1)
        return side

    def _sort_by_group(self, report_df: pd.DataFrame, group_df: pd.DataFrame):
        agcols = self._agcols
        lgcols = self._lgcols

        a_group_df = group_df[agcols].drop_duplicates(ignore_index=True).set_index(agcols)
        l_group_df = group_df[lgcols].drop_duplicates(ignore_index=True).set_index(lgcols)
        group_df = pd.concat([a_group_df, l_group_df], keys=['assets', 'liabs'])
        group_df['__sortcol__'] = range(0, len(group_df.index))

        report_df = (
            pd.merge(report_df, group_df, left_index=True, right_index=True, how='left', validate='one_to_one')
            .sort_values('__sortcol__')
            .drop('__sortcol__', axis=1)
        )
        return report_df
