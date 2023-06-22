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
    cell_repo = repository_postgres.CellRepo

    async def retrieve_sheet(self, data: entities.SheetRetrieve) -> entities.Sheet:
        sheet = await self.sheet_repo().retrieve_as_sheet(data=data)
        return sheet

    async def retrieve_scroll_size(self, sheet_id: core_types.Id_) -> entities.ScrollSize:
        scroll_size = await self.sheet_repo().retrieve_scroll_size(id_=sheet_id)
        return scroll_size

    async def retrieve_col_filter(self, data: entities.ColFilterRetrieve) -> entities.ColFilter:
        filter_items = await self.cell_repo().retrieve_filter_items({"col_id": data['col_id'], "is_index": False})
        col_filter = entities.ColFilter(col_id=data['col_id'], sheet_id=data['sheet_id'], items=filter_items)
        return col_filter

    async def update_col_filter(self, data: entities.ColFilter) -> None:
        await self.sheet_repo().update_col_filter(data)
