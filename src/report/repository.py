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
    repo: postgres.BaseRepo

    async def create(self, data: core_types.DTO) -> Entity:
        return await self.repo.create(data)

    async def retrieve_bulk(self, filter_by: dict, order_by: core_types.OrderBy) -> list[Entity]:
        return await self.repo.retrieve_bulk(filter_by, order_by)

    async def retrieve(self, filter_by: dict) -> Entity:
        return await self.repo.retrieve(filter_by)

    async def update(self, data: core_types.DTO, filter_by: dict) -> Entity:
        return await self.repo.update(data, filter_by)

    async def delete(self, filter_by: dict) -> core_types.Id_:
        return await self.repo.delete(filter_by)


class CategoryRepo(CrudRepo):
    repo = postgres.CategoryRepo()


class GroupRepo(CrudRepo):
    repo = postgres.GroupRepo()

    async def overwrite_linked_sheet(self, instance: entities.Group, data: entities.SheetCreate) -> None:
        await self.repo.overwrite_linked_sheet(instance, data)

    async def retrieve_linked_sheet_as_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        return await self.repo.retrieve_linked_sheet_as_dataframe(group_id)


class ReportRepo(CrudRepo):
    repo = postgres.ReportRepo()

    async def overwrite_linked_sheet(self, instance: entities.Report, data: entities.SheetCreate):
        await self.repo.overwrite_linked_sheet(instance, data)


class WireRepo(CrudRepo):
    repo = postgres.WireRepo()

    async def retrieve_wire_dataframe(self, filter_by: dict, order_by: core_types.OrderBy = None) -> pd.DataFrame:
        return await self.repo.retrieve_bulk_as_dataframe(filter_by, order_by)
