import pandas as pd
from abc import ABC, abstractmethod

from src.core_types import DTO, OrderBy, Id_
from .entities import Entity


class CrudRepository(ABC):
    @abstractmethod
    async def create_one(self, data: DTO) -> Entity:
        raise NotImplemented

    @abstractmethod
    async def get_one(self, filter_by: dict) -> Entity:
        raise NotImplemented

    @abstractmethod
    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                       slice_from: int = None, slice_to: int = None):
        raise NotImplemented

    @abstractmethod
    async def update_one(self, data: DTO, filter_by: dict) -> Entity:
        raise NotImplemented

    @abstractmethod
    async def delete_one(self, filter_by: dict) -> Id_:
        raise NotImplemented


class GroupRepository(CrudRepository, ABC):

    @abstractmethod
    async def get_linked_dataframe(self, group_id: Id_) -> pd.DataFrame:
        raise NotImplemented
