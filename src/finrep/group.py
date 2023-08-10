from copy import deepcopy

import loguru
import pandas as pd
from typing import Self
from abc import ABC, abstractmethod

from .wire import Wire


class Group(ABC):

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
    def __init__(self, group_df: pd.DataFrame, ccols: list[str], fixed_ccols: list[str] = None):
        self._group_df = group_df.copy()
        self._ccols = ccols.copy()
        self._fixed_ccols = fixed_ccols.copy() if fixed_ccols is not None else []

    @classmethod
    def from_wire(cls, wire: Wire, ccols: list[str], fixed_ccols: list[str] = None) -> Self:
        df = wire.get_wire_df()
        group_df = df[ccols].drop_duplicates().sort_values(ccols, ignore_index=True)
        levels = range(0, len(group_df.columns))
        for level in levels:
            name = f"level {level + 1}"
            group_df[name] = group_df.iloc[:, level]
        return cls(group_df, ccols, fixed_ccols)

    def get_group_df(self) -> pd.DataFrame:
        if self._group_df is None:
            raise Exception(f"group is None; you probably miss create_group(wire: Wire, ccols: list[str]) function")
        return self._group_df.copy()

    def update_group_df(self, wire: Wire) -> Self:
        if self._group_df is None:
            raise ValueError
        if self._ccols is None:
            raise ValueError
        if self._fixed_ccols is None:
            raise ValueError

        fixed_ccols = self._fixed_ccols

        old_group_df = self._group_df.copy()
        self._group_df = self.from_wire(wire, self._ccols, self._fixed_ccols).get_group_df()
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

    def create_group_df(self) -> Self:
        raise NotImplemented


class BalanceGroup(BaseGroup):
    __assets_key = 'assets'
    __liabs_key = 'liabs'

    def __init__(self, group_df: pd.DataFrame, ccols: list[str], fixed_ccols: list[str] = None):
        super().__init__(group_df, ccols, fixed_ccols)

    @classmethod
    def from_wire(cls, wire: Wire, ccols: list[str], fixed_ccols: list[str] = None) -> Self:
        df = wire.get_wire_df()
        group_df = df[ccols].drop_duplicates().sort_values(ccols, ignore_index=True)
        levels = range(0, len(group_df.columns))
        for level in levels:
            name = f"{cls.__assets_key}, level {level + 1}"
            group_df[name] = group_df.iloc[:, level]
        for level in levels:
            name = f"{cls.__liabs_key}, level {level + 1}"
            group_df[name] = group_df.iloc[:, level]
        return cls(group_df, ccols, fixed_ccols)

    def _rename_items(self, old_group_df: pd.DataFrame):
        ccols = self._ccols
        group_df = self._group_df
        length = len(ccols)
        for ccol, gcol in zip(ccols * 2, old_group_df.columns[length:length * 4]):
            mapper = pd.Series(old_group_df[gcol].tolist(), index=old_group_df[ccol].tolist()).to_dict()
            group_df[gcol] = group_df[gcol].replace(mapper)

    def get_splited_group_df(self) -> pd.DataFrame:
        names = [f"level {i}" for i in range(0, len(self._ccols) + 1)]
        agcols = [x for x in self._group_df.columns if self.__assets_key in x]
        lgcols = [x for x in self._group_df.columns if self.__liabs_key in x]

        assets: pd.DataFrame = self._group_df.copy()
        assets = assets.set_index(self._ccols)[agcols]
        assets.insert(0, names[0], self.__assets_key)
        assets.columns = names

        liabs = self._group_df.copy()
        liabs = liabs.set_index(self._ccols)[lgcols]
        liabs.insert(0, names[0], self.__liabs_key)
        liabs.columns = names

        splited = pd.concat([assets, liabs], keys=[self.__assets_key, self.__liabs_key], names=names)
        return splited
