import typing

import core_types

from . import schema
from .repository import Repository, PostgresRepo


class SheetService:
    repo: typing.Type[Repository] = PostgresRepo

    async def retrieve_sheet(self, data: schema.SheetRetrieveSchema) -> schema.SheetSchema:
        sheet_schema = await self.repo().retrieve_sheet(data)
        return sheet_schema

    async def retrieve_scroll_size(self, sheet_id: core_types.Id_) -> schema.ScrollSizeSchema:
        return schema.ScrollSizeSchema(
            count_cols=3,
            count_rows=50,
            scroll_height=1000,
            scroll_width=360,
        )
