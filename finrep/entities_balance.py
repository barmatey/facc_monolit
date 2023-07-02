import pandas as pd
from loguru import logger
from pandera.typing import DataFrame

from finrep import entities, entities_base, typing


class BalanceInterval(entities_base.BaseInterval):
    pass


class BalanceGroup(entities.Group):

    def __init__(self, wire_df: DataFrame[typing.WireSchema]):
        self.wire = wire_df.copy()
        self.group = None

    def create_group(self, ccols: list[str]) -> None:
        df = self.wire

        df = df[ccols].drop_duplicates().sort_values(ccols, ignore_index=True)

        # If you change name string be careful!
        # ReportBalance class is using this data
        levels = range(0, len(df.columns))
        for level in levels:
            name = f"Assets, level {level + 1}"
            df[name] = df.iloc[:, level]

        for level in levels:
            name = f"Liabilities, level {level + 1}"
            df[name] = df.iloc[:, level]

        self.group = df

    def get_group(self) -> pd.DataFrame:
        if self.group is None:
            raise Exception(f"group is None; you probably miss create_group() function")
        return self.group


class BalanceReport(entities_base.ReportBase):
    def create_report(self):
        df = self.merged_wire
        df["interval"] = pd.cut(df['date'], self.interval.get_intervals(), right=True)

        gcols_assets = [x for x in self.gcols if 'assets' in x.lower()]
        assets = self._create_balance_side(df, gcols_assets)

        gcols_liabs = [x for x in self.gcols if 'liabilities' in x.lower()]
        liabs = -1 * self._create_balance_side(df, gcols_liabs)

        report = pd.concat([assets, liabs], keys=['assets', 'liabilities'])
        report[report < 0] = 0
        report = self.drop_zero_rows(report)
        report = self._calculate_saldo(report)

        report = report.round(2)
        self.report = report

    def _create_balance_side(self, df: pd.DataFrame, gcols: list[str]) -> pd.DataFrame:
        group = df.groupby(gcols + ['interval'], as_index=False)
        side: pd.DataFrame = group[gcols + ['debit', 'credit']].sum(numeric_only=True)
        side['saldo'] = side['debit'] - side['credit']
        side = side.drop(['debit', 'credit'], axis=1).set_index(gcols)
        side = self._split_grouped_df_by_intervals(side)
        side = side.cumsum(axis=1)
        return side

    @staticmethod
    def _calculate_saldo(report: pd.DataFrame) -> pd.DataFrame:
        report = report.copy()
        index = ('saldo',) * len(report.index.levels)
        report.loc[index, :] = report.loc[('assets',), :].sum() - report.loc[('liabilities',), :].sum()
        return report
