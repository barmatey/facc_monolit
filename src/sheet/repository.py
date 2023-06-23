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

    @abstractmethod
    async def retrieve_col_filter(self, data: entities.ColFilterRetrieve) -> entities.ColFilter:
        pass

    @abstractmethod
    async def update_col_filter(self, data: entities.ColFilter) -> None:
        pass


class PostgresRepo(Repository):
    sheet_repo = repository_postgres.SheetRepo
    sheet_filter_repo = repository_postgres.SheetFilterRepo

    async def retrieve_sheet(self, data: entities.SheetRetrieve) -> entities.Sheet:
        sheet = await self.sheet_repo().retrieve_as_sheet(data=data)
        return sheet

    async def retrieve_scroll_size(self, sheet_id: core_types.Id_) -> entities.ScrollSize:
        scroll_size = await self.sheet_repo().retrieve_scroll_size(id_=sheet_id)
        return scroll_size

    async def retrieve_col_filter(self, data: entities.ColFilterRetrieve) -> entities.ColFilter:
        return await self.sheet_filter_repo().retrieve_col_filter(sheet_id=data['sheet_id'], col_id=data['col_id'])

    async def update_col_filter(self, data: entities.ColFilter) -> None:
        await self.sheet_filter_repo().update_col_filter(data)
