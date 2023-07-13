import typing
from copy import deepcopy

import pandas as pd
import numpy as np


class Interval:

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
