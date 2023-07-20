from abc import ABC, abstractmethod

import pandas as pd
import pydantic

from src import core_types
from repository_postgres import SourceRepo, WireRepo
from . import entities, schema


class Repository(ABC):

    @abstractmethod
    async def create(self, data: pydantic.BaseModel) -> entities.Entity:
        pass

    @abstractmethod
    async def retrieve(self, filter_by: dict) -> entities.Entity:
        pass

    @abstractmethod
    async def update(self, filter_by: dict, data: pydantic.BaseModel) -> entities.Entity:
        pass

    @abstractmethod
    async def retrieve_list(self, retrieve_params: core_types.DTO) -> list[entities.Entity]:
        pass

    @abstractmethod
    async def delete(self, filter_by: dict) -> None:
        pass


class SourcePostgres(Repository):
    source_repo = SourceRepo

    async def create(self, data: entities.SourceCreate) -> entities.Entity:
        return await self.source_repo().create(data)

    async def retrieve(self, filter_by: dict) -> entities.Entity:
        return await self.source_repo().retrieve(filter_by)

    async def update(self, filter_by: dict, data: pydantic.BaseModel) -> entities.Entity:
        return await self.source_repo().update(data, filter_by)

    async def delete(self, filter_by: dict) -> None:
        await self.source_repo().delete(filter_by)

    async def retrieve_list(self, retrieve_params: schema.WireBulkRetrieveSchema) -> list[entities.Entity]:
        return await self.source_repo().retrieve_bulk(**retrieve_params.dict())


class WirePostgres(Repository):
    wire_repo = WireRepo

    async def create(self, data: entities.WireCreate) -> pydantic.BaseModel:
        return await self.wire_repo().create(data)

    async def retrieve(self, filter_by: dict) -> pydantic.BaseModel:
        raise NotImplemented

    async def update(self, filter_by: dict, data: pydantic.BaseModel) -> entities.Wire:
        return await self.wire_repo().update(data, filter_by)

    async def delete(self, filter_by: dict) -> None:
        await self.wire_repo().delete(filter_by)

    async def retrieve_list(self, retrieve_params: schema.WireBulkRetrieveSchema) -> list[entities.Wire]:
        return await self.wire_repo().retrieve_bulk(**retrieve_params.dict())

    async def retrieve_bulk_as_dataframe(self, filter_by: dict, order_by: core_types.OrderBy = None) -> pd.DataFrame:
        return await self.wire_repo().retrieve_bulk_as_dataframe(filter_by, order_by)
