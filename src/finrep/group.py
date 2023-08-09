from copy import deepcopy

import loguru
import pandas as pd
from typing import Self
from abc import ABC, abstractmethod

from .wire import Wire


class Group(ABC):

    @abstractmethod
    def create_group_df(self, wire: Wire, ccols: list[str]) -> Self:
        raise NotImplemented

    @abstractmethod
    def update_group_df(self, wire: Wire) -> Self:
        raise NotImplemented

    @abstractmethod
    def get_group_df(self) -> pd.DataFrame:
        raise NotImplemented

    @abstractmethod
    def copy(self) -> Self:
        raise NotImplemented


class BaseGroup(Group, ABC):
    def __init__(self, df: pd.DataFrame = None, ccols: list[str] = None, fixed_ccols: list[str] = None):
        self._group_df = df.copy() if df is not None else None
        self._ccols = ccols.copy() if ccols is not None else None
        self._fixed_ccols = fixed_ccols.copy() if fixed_ccols is not None else []

    def get_group_df(self) -> pd.DataFrame:
        if self._group_df is None:
            raise Exception(f"group is None; you probably miss create_group(wire: Wire, ccols: list[str]) function")
        return self._group_df

    def update_group_df(self, wire: Wire) -> Self:
        if self._group_df is None:
            raise ValueError
        if self._ccols is None:
            raise ValueError
        if self._fixed_ccols is None:
            raise ValueError

        fixed_ccols = self._fixed_ccols
        ccols = self._ccols

        old_group_df = self._group_df.copy()
        self.create_group_df(wire, ccols)

        if len(fixed_ccols):
            self._group_df = pd.merge(
                old_group_df[fixed_ccols].drop_duplicates(),
                self._group_df,
                on=fixed_ccols,
                how='left',
            )
        self._rename_items(old_group_df)
        return self

    def _rename_items(self, old_group_df: pd.DataFrame):
        ccols = self._ccols
        group_df = self._group_df
        length = len(ccols)
        for ccol, gcol in zip(ccols, old_group_df.columns[length:length * 2]):
            mapper = pd.Series(old_group_df[gcol].tolist(), index=old_group_df[ccol].tolist()).to_dict()
            group_df[gcol] = group_df[gcol].replace(mapper)

    def copy(self) -> Self:
        return deepcopy(self)


class ProfitGroup(BaseGroup):

    def create_group_df(self, wire: Wire, ccols: list[str]) -> Self:
        raise NotImplemented


class BalanceGroup(BaseGroup):

    def __init__(self, df: pd.DataFrame = None, ccols: list[str] = None, fixed_ccols: list[str] = None):
        super().__init__(df, ccols, fixed_ccols)
        self.__assets_key = 'assets'
        self.__liabs_key = 'liabs'

    def create_group_df(self, wire: Wire, ccols: list[str]) -> Self:
        df = wire.get_wire_df()
        group_df = df[ccols].drop_duplicates().sort_values(ccols, ignore_index=True)

        levels = range(0, len(group_df.columns))
        for level in levels:
            name = f"{self.__assets_key}, level {level + 1}"
            group_df[name] = group_df.iloc[:, level]

        for level in levels:
            name = f"{self.__liabs_key}, level {level + 1}"
            group_df[name] = group_df.iloc[:, level]

        self._group_df = group_df
        self._ccols = ccols.copy()
        return self

    def _rename_items(self, old_group_df: pd.DataFrame):
        ccols = self._ccols
        group_df = self._group_df
        length = len(ccols)
        for ccol, gcol in zip(ccols * 2, old_group_df.columns[length:length * 4]):
            mapper = pd.Series(old_group_df[gcol].tolist(), index=old_group_df[ccol].tolist()).to_dict()
            group_df[gcol] = group_df[gcol].replace(mapper)
