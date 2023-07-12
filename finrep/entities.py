import typing
from abc import ABC, abstractmethod
import numpy as np
import pandas as pd
import pandera as pa


class Interval(ABC):

    @abstractmethod
    def get_total_start_date(self) -> np.datetime64:
        pass

    @abstractmethod
    def get_total_end_date(self) -> np.datetime64:
        pass

    @abstractmethod
    def get_start_date(self) -> np.datetime64:
        pass

    @abstractmethod
    def get_end_date(self) -> np.datetime64:
        pass

    def get_intervals(self) -> pd.DatetimeIndex:
        pass

    @abstractmethod
    def copy(self) -> typing.Self:
        pass


class Group(ABC):
    @abstractmethod
    async def create_group(self, ccols: list[str]) -> None:
        pass

    @abstractmethod
    async def get_group(self) -> pd.DataFrame:
        pass


class Report(ABC):

    @abstractmethod
    async def create_report(self):
        pass

    @abstractmethod
    async def get_report(self) -> pd.DataFrame:
        pass


class WireSchema(pa.DataFrameModel):
    date: pa.typing.Series[pd.Timestamp]
    sender: pa.typing.Series[float]
    receiver: pa.typing.Series[float]
    debit: pa.typing.Series[float]
    credit: pa.typing.Series[float]
