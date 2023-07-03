import enum
from abc import ABC, abstractmethod

import loguru
import pandas as pd

import finrep

from . import schema, enums


class FinrepService(ABC):

    @abstractmethod
    async def create_group(self, wire: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        pass

    @abstractmethod
    async def create_report(self, data: schema.ReportCreateSchema) -> pd.DataFrame:
        pass


class BalanceService(FinrepService):

    async def create_group(self, wire: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        balance = finrep.BalanceGroup(wire)
        balance.create_group(ccols=columns)
        balance_group: pd.DataFrame = balance.get_group()
        return balance_group

    async def create_report(self, data: schema.ReportCreateSchema) -> pd.DataFrame:
        raise NotImplemented


class LinkedFinrep(enum.Enum):
    BALANCE = BalanceService


def get_finrep_service(category: enums.CategoryLiteral) -> FinrepService:
    return LinkedFinrep[category].value()
