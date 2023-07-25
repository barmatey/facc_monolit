from abc import ABC, abstractmethod
import pandas as pd
import typing

from .my_order import OrderBy

from .my_dto import DTO

Entity = typing.TypeVar("Entity")


class Repository(ABC):

    @abstractmethod
    async def create_one(self, data: DTO) -> Entity:
        pass

    @abstractmethod
    async def create_many(self, data: list[DTO], without_return=False) -> list[Entity]:
        pass

    @abstractmethod
    async def get_one(self, filter_by: dict) -> Entity:
        pass

    @abstractmethod
    async def get_many(self, filter_by: dict, order_by: OrderBy, asc: bool = True) -> list[Entity]:
        pass

    @abstractmethod
    async def get_many_as_frame(self, filter_by: dict, order_by: OrderBy, asc: bool = True) -> pd.DataFrame:
        pass

    @abstractmethod
    async def get_many_as_dicts(self, filter_by: dict, order_by: OrderBy, asc=True) -> list[dict]:
        pass

    @abstractmethod
    async def get_many_as_records(self, filter_by: dict, order_by: OrderBy, asc=True) -> list[tuple]:
        pass

    @abstractmethod
    async def update_one(self, data: DTO, filter_by: dict) -> Entity:
        pass

    @abstractmethod
    async def update_many(self, data: list[DTO], mapper: dict, filter_by: dict, without_return=False) -> list[Entity]:
        pass
