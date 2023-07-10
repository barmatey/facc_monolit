import typing
from abc import ABC, abstractmethod

import pydantic

from src import core_types
from . import repository, entities


class Service(ABC):
    repo: typing.Type[repository.Repository]

    async def create(self, data: pydantic.BaseModel) -> entities.Entity:
        return await self.repo().create(data)

    async def retrieve(self, filter_by: dict) -> entities.Entity:
        return await self.repo().retrieve(filter_by)

    async def update(self, filter_by: dict, data: pydantic.BaseModel) -> entities.Entity:
        return await self.repo().update(filter_by, data)

    async def delete(self, filter_by: dict) -> None:
        await self.repo().delete(filter_by)

    async def retrieve_list(self, retrieve_params: core_types.DTO) -> list[entities.Entity]:
        return await self.repo().retrieve_list(retrieve_params)


class ServiceSource(Service):
    repo: repository.Repository = repository.SourcePostgres


class ServiceWire(Service):
    repo: repository.Repository = repository.WirePostgres
