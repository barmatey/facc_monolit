import typing
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
    pass


class GroupService(Service):

    async def create(self, data: schema.GroupCreateSchema) -> entities.Group:
        finrep_service = get_finrep_service(data.category)
        group = await finrep_service.create_group(data)
        return group


class ReportSrvice(Service):
    async def create(self, data: schema.ReportCreateSchema) -> entities.Report:
        finrep_service = get_finrep_service(data.category)
        report = await finrep_service.create_report(data)
        return report
