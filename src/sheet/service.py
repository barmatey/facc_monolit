import typing

import core_types

from . import schema
from .repository import Repository, PostgresRepo


class SheetService:
    repo: typing.Type[Repository] = PostgresRepo

    async def retrieve_sheet(self, data: schema.SheetRetrieveSchema) -> schema.SheetSchema:
        sheet_schema = await self.repo().retrieve_sheet(data)
        return sheet_schema
