import typing
from pydantic import BaseModel

from src import core_types
from . import repository_new as repository

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
