import typing
from abc import ABC, abstractmethod

import pandas as pd
from pydantic import BaseModel

from src import core_types
from src import repository_postgres as postgres
from . import entities, schema

Entity = typing.TypeVar(
    'Entity',
    entities.Group,
    entities.Report,
    entities.ReportCategory,
)


class CrudRepo(ABC):
    @abstractmethod
    async def create(self, data: core_types.DTO) -> Entity:
        pass

    @abstractmethod
    async def retrieve_bulk(self, filter_by: dict, order_by: core_types.OrderBy) -> list[Entity]:
        pass

    @abstractmethod
    async def retrieve(self, filter_by: dict) -> Entity:
        pass

    @abstractmethod
    async def update(self, instance: Entity, data: core_types.DTO) -> Entity:
        pass

    @abstractmethod
    async def delete(self, filter_by: dict) -> core_types.Id_:
        pass


class CategoryRepo(postgres.CategoryRepo, CrudRepo):
    pass


class GroupRepo(CrudRepo):
    repo = postgres.GroupRepo

    async def create(self, data: entities.GroupCreate) -> Entity:
        return await self.repo().create(data)

    async def retrieve_bulk(self, filter_by: dict, order_by: core_types.OrderBy) -> list[entities.Group]:
        return await self.repo().retrieve_bulk(filter_by, order_by)

    async def retrieve(self, filter_by: dict) -> entities.Group:
        return await self.repo().retrieve(filter_by)

    async def update(self, instance: Entity, data: core_types.DTO) -> entities.Group:
        raise NotImplemented

    async def update_sheet(self, instance: Entity, data: schema.GroupSheetUpdateSchema) -> entities.Group:
        return await self.repo().update_sheet(instance, data)

    async def delete(self, filter_by: dict) -> core_types.Id_:
        return await self.repo().delete(filter_by)

    async def retrieve_linked_sheet_as_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        return await self.repo().retrieve_linked_sheet_as_dataframe(group_id)


class ReportRepo(postgres.ReportRepo, CrudRepo):
    pass


class WireRepo(postgres.WireRepo):
    async def retrieve_wire_dataframe(self, filter_by: dict, order_by: core_types.OrderBy = None) -> pd.DataFrame:
        return await self.retrieve_bulk_as_dataframe(filter_by, order_by)
