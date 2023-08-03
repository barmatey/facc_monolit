import loguru
import numpy as np
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from .wire import Wire
from .group import Group, ProfitGroup, BalanceGroup
from .interval import Interval


class Report:
    def __init__(self, df: pd.DataFrame = None):
        self.report = df.copy() if df is not None else None

    def create_report(self, wire: Wire, group: Group, interval: Interval) -> None:
        raise NotImplemented

    def get_report(self) -> pd.DataFrame:
        if self.report is None:
            raise Exception('report is None; Did you forgot create_report() function?')
        return self.report

    @staticmethod
    def _find_ccols(wire_cols: list[str], group_cols: list[str]) -> list[str]:
        ccols = [x for x in group_cols if x in wire_cols]
        if len(ccols) == 0:
            raise Exception("there are no intersections")

        return ccols

    @staticmethod
    def _find_gcols(wire_cols: list[str], group_cols: list[str]) -> list[str]:
        gcols = [x for x in group_cols if x not in wire_cols]
        if len(gcols) == 0:
            raise Exception("there are no intersections")
        return gcols

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

    @staticmethod
    def _merge_wire_df_with_group_df(wire: pd.DataFrame, group: pd.DataFrame, ccols: list[str]):
        wire = wire.copy()
        group = group.copy()

        wire.loc[:, ccols] = wire.loc[:, ccols].astype(str)
        group.loc[:, ccols] = group.loc[:, ccols].astype(str)

        merged = pd.merge(wire, group, on=ccols, how='inner')
        return merged

    @staticmethod
    def _group_wires_by_gcols_and_intervals(df: pd.DataFrame, interval: Interval,
                                                  gcols: list[str]) -> DataFrameGroupBy:
        df["interval"] = pd.cut(df['date'], interval.get_intervals(), right=True)
        grouped_wires = df.groupby(gcols + ['interval'], as_index=False)
        return grouped_wires

    @staticmethod
    def _calculate_saldo_from_grouped_wires(grouped_wires: DataFrameGroupBy, gcols: list[str]) -> pd.DataFrame:
        result: pd.DataFrame = grouped_wires[gcols + ['debit', 'credit']].sum(numeric_only=True)
        result['saldo'] = result['debit'] - result['credit']
        result = result.drop(['debit', 'credit'], axis=1).set_index(gcols)
        return result

    @staticmethod
    def _drop_zero_rows(df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace(0, np.nan)
        df = df.dropna(axis=0, how='all')
        df = df.replace(np.nan, 0)
        return df


class ProfitReport(Report):
    def create_report(self, wire: Wire, group: ProfitGroup, interval: Interval) -> None:
        wire_df = wire.get_wire_df()
        group_df = group.get_group_df()

        ccols = super()._find_ccols(wire_df.columns, group_df.columns)
        gcols = super()._find_gcols(wire_df.columns, group_df.columns)
        gcols.pop(gcols.index('reverse'))

        merged_wires = super()._merge_wire_df_with_group_df(wire_df, group_df, ccols)
        merged_wires.loc[merged_wires['reverse'], 'debit'] = -1 * merged_wires.loc[merged_wires['reverse'], 'debit']
        merged_wires.loc[merged_wires['reverse'], 'credit'] = -1 * merged_wires.loc[merged_wires['reverse'], 'credit']
        merged_wires = merged_wires.drop('reverse', axis=1)

        grouped_wires = super()._group_wires_by_gcols_and_intervals(merged_wires, interval, gcols)

        report = super()._calculate_saldo_from_grouped_wires(grouped_wires, gcols)
        report = super()._split_df_by_intervals(report)
        report = super()._drop_zero_rows(report)

        report = report.round(2)
        self.report = report


class BalanceReport(Report):
    def create_report(self, wire: Wire, group: BalanceGroup, interval: Interval) -> None:
        wire_df = wire.get_wire_df()
        group_df = group.get_group_df()

        ccols = super()._find_ccols(wire_df.columns, group_df.columns)
        gcols = super()._find_gcols(wire_df.columns, group_df.columns)

        merged_wires = super()._merge_wire_df_with_group_df(wire_df, group_df, ccols)
        merged_wires["interval"] = pd.cut(merged_wires['date'], interval.get_intervals(), right=True)

        assets = self._create_balance_side(merged_wires, gcols=group.get_gcols_assets(gcols))
        liabs = -1 * self._create_balance_side(merged_wires, gcols=group.get_gcols_liabs(gcols))

        report = pd.concat([assets, liabs], keys=['assets', 'liabs'])
        report[report < 0] = 0
        report = super()._drop_zero_rows(report)

        saldo = assets.sum() + liabs.sum()
        report.loc[('saldo',) * len(report.index.levels), :] = saldo

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
