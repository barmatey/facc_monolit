from abc import ABC, abstractmethod

import pydantic

from src import core_types
from repository_postgres import SourceRepo, WireRepo
from . import entities, schema


class Repository(ABC):

    @abstractmethod
    async def create(self, data: pydantic.BaseModel) -> pydantic.BaseModel:
        pass

    @abstractmethod
    async def retrieve(self, filter_by: dict) -> pydantic.BaseModel:
        pass

    @abstractmethod
    async def retrieve_list(self, retrieve_params: core_types.DTO) -> list[pydantic.BaseModel]:
        pass

    @abstractmethod
    async def delete(self, filter_by: dict) -> None:
        pass


class SourcePostgres(Repository):
    source_repo = SourceRepo

    async def create(self, data: entities.SourceCreate) -> entities.Source:
        return await self.source_repo().create(data)

    async def retrieve(self, filter_by: dict) -> entities.Source:
        return await self.source_repo().retrieve(filter_by)

    async def delete(self, filter_by: dict) -> None:
        await self.source_repo().delete(filter_by)

    async def retrieve_list(self, retrieve_params: schema.WireBulkRetrieveSchema) -> list[entities.Source]:
        return await self.source_repo().retrieve_bulk(**retrieve_params.dict())


class WirePostgres(Repository):
    wire_repo = WireRepo

    async def create(self, data: pydantic.BaseModel) -> pydantic.BaseModel:
        raise NotImplemented

    async def retrieve(self, filter_by: dict) -> pydantic.BaseModel:
        raise NotImplemented

    async def delete(self, filter_by: dict) -> None:
        raise NotImplemented

    async def retrieve_list(self, retrieve_params: schema.WireBulkRetrieveSchema) -> list[entities.Wire]:
        return await self.wire_repo().retrieve_bulk(**retrieve_params.dict())
