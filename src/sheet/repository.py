from abc import ABC, abstractmethod

import core_types
from . import entities
import repository_postgres


class Repository(ABC):
    @abstractmethod
    async def retrieve_sheet(self, data: entities.SheetRetrieve) -> entities.Sheet:
        pass

    @abstractmethod
    async def retrieve_scroll_size(self, sheet_id: core_types.Id_) -> entities.ScrollSize:
        pass


class PostgresRepo(Repository):
    repo = repository_postgres.SheetRepo

    async def retrieve_sheet(self, data: entities.SheetRetrieve) -> entities.Sheet:
        sheet = await self.repo().retrieve_as_sheet(data=data)
        return sheet

    async def retrieve_scroll_size(self, sheet_id: core_types.Id_) -> entities.ScrollSize:
        scroll_size = await self.repo().retrieve_scroll_size(id_=sheet_id)
        return scroll_size

