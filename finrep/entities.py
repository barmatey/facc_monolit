import typing
from abc import ABC, abstractmethod
import numpy as np
import pandas as pd


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
    def create_group(self, ccols: list[str]) -> None:
        pass

    @abstractmethod
    def get_group(self) -> pd.DataFrame:
        pass


class Report(ABC):

    @abstractmethod
    def create_report(self):
        pass

    @abstractmethod
    def get_report(self) -> pd.DataFrame:
        pass
