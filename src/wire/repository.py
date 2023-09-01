from abc import ABC, abstractmethod

import pandas as pd

from src import core_types
from src.core_types import DTO, OrderBy
from . import entities


class RepositoryCrud(ABC):

    @abstractmethod
    async def create_one(self, data: DTO) -> entities.Entity:
        pass

    @abstractmethod
    async def create_many(self, data: list[DTO]) -> None:
        pass

    @abstractmethod
    async def get_one(self, filter_by: dict) -> entities.Entity:
        pass

    @abstractmethod
    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                       slice_from: int = None, slice_to: int = None) -> list[entities.Entity]:
        pass

    @abstractmethod
    async def get_uniques(self, columns_by: list[str], filter_by: dict,
                          order_by: OrderBy = None, asc=True, ) -> list[dict]:
        raise NotImplemented

    @abstractmethod
    async def get_many_as_frame(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                                slice_from: int = None, slice_to: int = None) -> pd.DataFrame:
        pass

    @abstractmethod
    async def update_one(self, data: DTO, filter_by: dict, ) -> entities.Entity:
        pass

    @abstractmethod
    async def update_many_via_id(self, data: list[DTO]) -> None:
        raise NotImplemented

    @abstractmethod
    async def delete_one(self, filter_by: dict) -> entities.Entity:
        pass

    @abstractmethod
    async def delete_many(self, filter_by: dict) -> None:
        raise NotImplemented
