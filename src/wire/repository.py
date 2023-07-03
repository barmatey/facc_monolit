from abc import ABC, abstractmethod

import pydantic

from src import core_types
from src.repository_postgres_new import SourceRepo
from . import entities


class Repository(ABC):

    @abstractmethod
    async def create(self, data: pydantic.BaseModel) -> pydantic.BaseModel:
        pass

    @abstractmethod
    async def retrieve(self, source_id: core_types.Id_) -> pydantic.BaseModel:
        pass

    @abstractmethod
    async def delete(self, source_id: core_types.Id_) -> None:
        pass

    @abstractmethod
    async def list(self) -> list[pydantic.BaseModel]:
        pass


class RepositoryPostgres(Repository):
    source_repo = SourceRepo

    async def create(self, data: entities.SourceCreate) -> entities.Source:
        return await self.source_repo().create(data)

    async def retrieve(self, source_id: core_types.Id_) -> entities.Source:
        return await self.source_repo().retrieve(source_id)

    async def delete(self, source_id: core_types.Id_) -> None:
        await self.source_repo().delete({"id": source_id})

    async def list(self) -> list[entities.Source]:
        return await self.source_repo().retrieve_bulk({})
