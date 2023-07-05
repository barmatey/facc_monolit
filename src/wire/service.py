import typing
from abc import ABC, abstractmethod

import pydantic

from src import core_types
from . import repository


class Service(ABC):
    repo: typing.Type[repository.Repository]

    async def create(self, data: pydantic.BaseModel) -> pydantic.BaseModel:
        return await self.repo().create(data)

    async def retrieve(self, filter_by: dict) -> pydantic.BaseModel:
        return await self.repo().retrieve(filter_by)

    async def delete(self, filter_by: dict) -> None:
        await self.repo().delete(filter_by)

    async def retrieve_list(self, filter_by: dict) -> list[pydantic.BaseModel]:
        return await self.repo().retrieve_list(filter_by)


class ServiceSource(Service):
    repo: repository.Repository = repository.SourcePostgres
