import typing

import pandas as pd
from pydantic import BaseModel

from src import core_types
from .service_finrep import get_finrep_service
from . import repository_new as repository
from . import enums, entities, schema

OrderBy = typing.Union[str, list[str]]
DTO = typing.Union[BaseModel]


class Service:
    repo: repository.CrudRepo

    async def create(self, data: BaseModel) -> BaseModel:
        return await self.repo.create(data)

    async def retrieve(self, filter_by: dict) -> BaseModel:
        return await self.repo.retrieve(filter_by)

    async def retrieve_bulk(self, filter_by: dict, order_by: OrderBy = None) -> list[BaseModel]:
        return await self.repo.retrieve_bulk(filter_by, order_by)

    async def partial_update(self, data: BaseModel, filter_by: dict) -> BaseModel:
        raise NotImplemented

    async def delete(self, filter_by: dict) -> core_types.Id_:
        return await self.repo.delete(filter_by)


class CategoryService(Service):
    repo: repository.CrudRepo = repository.CategoryRepo()


class GroupService(Service):
    repo: repository.CrudRepo = repository.GroupRepo()
    wire_repo: repository.WireRepo = repository.WireRepo()

    async def create(self, data: schema.GroupCreateSchema) -> entities.Group:
        wire: pd.DataFrame = await self.wire_repo.retrieve_wire_dataframe({"source_id": data.source_id})
        group: pd.DataFrame = await get_finrep_service(data.category).create_group(wire, data.columns)

        group_create = entities.GroupCreate(
            title=data.title,
            source_id=data.source_id,
            columns=data.columns,
            dataframe=group,
            drop_index=True,
            drop_columns=False,
            category=data.category,
        )

        group: entities.Group = await self.repo.create(group_create)
        return group


class ReportService(Service):
    repo: repository.CrudRepo = repository.ReportRepo()
    wire_repo: repository.WireRepo = repository.WireRepo()

    async def create(self, data: schema.ReportCreateSchema) -> entities.Report:

        raise NotImplemented
