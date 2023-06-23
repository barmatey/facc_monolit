import typing

import core_types

from . import schema
from .repository import Repository, PostgresRepo


class SheetService:
    repo: typing.Type[Repository] = PostgresRepo

    async def retrieve_sheet(self, data: schema.SheetRetrieveSchema) -> schema.SheetSchema:
        sheet_schema = await self.repo().retrieve_sheet(data=data)
        return sheet_schema

    async def retrieve_scroll_size(self, sheet_id: core_types.Id_) -> schema.ScrollSizeSchema:
        scroll_size = await self.repo().retrieve_scroll_size(sheet_id=sheet_id)
        scroll_size = schema.ScrollSizeSchema.from_scroll_size_entity(scroll_size)
        return scroll_size

    async def retrieve_col_filter(self, data: schema.ColFilterRetrieveSchema) -> schema.ColFilterSchema:
        col_filter = await self.repo().retrieve_col_filter(data)
        return col_filter

    async def update_col_filter(self, data: schema.ColFilterSchema) -> None:
        await self.repo().update_col_filter(data)

    async def update_col_sorter(self, data: schema.ColSorterSchema) -> None:
        await self.repo().update_col_sorter(data)

