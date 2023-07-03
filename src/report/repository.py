import typing
from abc import ABC, abstractmethod

import pandas as pd
from pydantic import BaseModel

from src import core_types
from src import repository_postgres as postgres
from . import entities

OrderBy = typing.Union[str, list[str]]
DTO = typing.Union[BaseModel, dict]
Entity = typing.TypeVar(
    'Entity',
    entities.Group,
    entities.Report,
)


class CrudRepo(ABC):
    @abstractmethod
    async def create(self, data: DTO) -> Entity:
        pass

    @abstractmethod
    async def retrieve(self, filter_by: dict) -> Entity:
        pass

    @abstractmethod
    async def retrieve_bulk(self, filter_by: dict, order_by: OrderBy) -> list[Entity]:
        pass

    @abstractmethod
    async def delete(self, filter_by: dict) -> core_types.Id_:
        pass


class CategoryRepo(postgres.CategoryRepo, CrudRepo):
    pass


class GroupRepo(postgres.GroupRepo, CrudRepo):
    pass


class ReportRepo(postgres.ReportRepo, CrudRepo):
    pass


class WireRepo(postgres.WireRepo):
    async def retrieve_wire_dataframe(self, filter_by: dict, order_by: OrderBy = None) -> pd.DataFrame:
        return await self.retrieve_bulk_as_dataframe(filter_by, order_by)
