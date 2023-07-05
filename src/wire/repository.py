from abc import ABC, abstractmethod

import pydantic

from src import core_types
from repository_postgres import SourceRepo
from . import entities


class Repository(ABC):

    @abstractmethod
    async def create(self, data: pydantic.BaseModel) -> pydantic.BaseModel:
        pass

    @abstractmethod
    async def retrieve(self, filter_by: dict) -> pydantic.BaseModel:
        pass

    @abstractmethod
    async def delete(self, filter_by: dict) -> None:
        pass

    @abstractmethod
    async def list(self) -> list[pydantic.BaseModel]:
        pass


class RepositoryPostgres(Repository):
    source_repo = SourceRepo

    async def create(self, data: entities.SourceCreate) -> entities.Source:
        return await self.source_repo().create(data)

    async def retrieve(self, filter_by: dict) -> entities.Source:
        return await self.source_repo().retrieve(filter_by)

    async def delete(self, filter_by: dict) -> None:
        await self.source_repo().delete(filter_by)

    async def list(self) -> list[entities.Source]:
        return await self.source_repo().retrieve_bulk({})
