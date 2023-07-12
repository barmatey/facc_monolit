import typing
import numpy as np
import pandas as pd
from copy import deepcopy

from pandera.typing import DataFrame

from . import entities, types


class BaseInterval(entities.Interval):

    def __init__(self,
                 iyear: int,
                 imonth: int,
                 iday: int,
                 start_date: pd.Timestamp,
                 end_date: pd.Timestamp,
                 total_start_date=None,
                 total_end_date=None
                 ):
        self.years = iyear
        self.months = imonth
        self.days = iday
        self.start_date = np.datetime64(start_date)
        self.end_date = np.datetime64(end_date)
        self.total_start_date = np.datetime64(total_start_date) if total_start_date is not None else None
        self.total_end_date = np.datetime64(total_end_date) if total_end_date is not None else None

        if self.days:
            freq = f"{self.days}D"
        else:
            freq = f"{self.months}M"
        self.intervals = pd.date_range(start=self.start_date, end=self.end_date, freq=freq)

    def get_start_date(self) -> np.datetime64:
        return self.start_date

    def get_end_date(self) -> np.datetime64:
        return self.end_date

    def get_total_start_date(self) -> np.datetime64:
        return self.total_start_date

    def get_total_end_date(self) -> np.datetime64:
        return self.total_end_date

    def get_intervals(self) -> pd.DatetimeIndex:
        return self.intervals

    def copy(self) -> typing.Self:
        return deepcopy(self)


class BaseGroup(entities.Group):

    def __init__(self, wire_df):
        self.wire = wire_df.copy()
        self.group = None

    async def create_group(self, ccols: list[str]) -> None:
        pass

    async def get_group(self) -> pd.DataFrame:
        if self.group is None:
            raise Exception(f"group is None; you probably miss create_group() function")
        return self.group


class BaseReport(entities.Report):

    def __init__(self, wire: DataFrame[types.WireSchema], group: pd.DataFrame, interval: BaseInterval):
        self.ccols = self._find_ccols(wire.columns, group.columns)
        self.gcols = self._find_gcols(wire.columns, group.columns)
        self.merged_wire = self._merge_wire_with_group(wire, group, self.ccols)
        self.interval = interval.copy()

        self.report: pd.DataFrame | None = None

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
    def _split_grouped_df_by_intervals(df: pd.DataFrame) -> pd.DataFrame:
        if 'interval' not in df.columns:
            raise ValueError('"interval" not in df.columns')
        if len(df.columns) > 2:
            raise ValueError('function expected df with to columns only (and the first column must be "interval")')

        splited = []
        columns = []

        for interval in df['interval'].unique():
            series = df.loc[df['interval'] == interval].drop('interval', axis=1)
            splited.append(series)
            columns.append(str(interval.right.date()))

        splited = pd.concat(splited, axis=1)
        splited.columns = columns
        return splited

    @staticmethod
    def _merge_wire_with_group(wire: DataFrame[types.WireSchema], group: pd.DataFrame, ccols: list[str]):
        wire = wire.copy()
        group = group.copy()

        wire.loc[:, ccols] = wire.loc[:, ccols].astype(str)
        group.loc[:, ccols] = group.loc[:, ccols].astype(str)

        merged = pd.merge(wire, group, on=ccols, how='inner')
        return merged

    @staticmethod
    def drop_zero_rows(df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace(0, np.nan)
        df = df.dropna(axis=0, how='all')
        df = df.replace(np.nan, 0)
        return df

    def create_report(self):
        raise NotImplemented

    def get_report(self) -> pd.DataFrame:
        if self.report is None:
            raise Exception('report is None; Did you forgot create_report() function?')
        return self.report
