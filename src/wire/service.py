import typing
from abc import ABC, abstractmethod

import pydantic

from src import core_types
from . import repository


class Service(ABC):
    repo: typing.Type[repository.Repository]

    async def create(self, data: pydantic.BaseModel) -> pydantic.BaseModel:
        return await self.repo().create(data)

    async def retrieve(self, source_id: core_types.Id_) -> pydantic.BaseModel:
        return await self.repo().retrieve(source_id)

    async def delete(self, id_: core_types.Id_) -> None:
        await self.repo().delete(id_)

    async def list(self) -> list[pydantic.BaseModel]:
        return await self.repo().list()


class ServiceSource(Service):
    repo: repository.Repository = repository.RepositoryPostgres
