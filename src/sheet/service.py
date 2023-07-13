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

    async def clear_all_filters(self, sheet_id: core_types.Id_) -> None:
        await self.repo().clear_all_filters(sheet_id)

    async def update_col_sorter(self, data: schema.ColSorterSchema) -> None:
        await self.repo().update_col_sorter(data)

    async def update_col_size(self, sheet_id: core_types.Id_, data: schema.UpdateSindexSizeSchema) -> None:
        await self.repo().update_col_size(sheet_id, data)

    async def copy_rows(self, sheet_id: core_types.Id_,
                        copy_from: list[schema.CopySindexSchema],
                        copy_to: list[schema.CopySindexSchema]) -> None:
        await self.repo().copy_rows(sheet_id, copy_from, copy_to)

    async def copy_cols(self, sheet_id: core_types.Id_,
                        copy_from: list[schema.CopySindexSchema],
                        copy_to: list[schema.CopySindexSchema]) -> None:
        await self.repo().copy_cols(sheet_id, copy_from, copy_to)

    async def copy_cells(self, sheet_id: core_types.Id_,
                         copy_from: list[schema.CopyCellSchema], copy_to: list[schema.CopyCellSchema]) -> None:
        await self.repo().copy_cells(sheet_id, copy_from, copy_to)

    async def update_cell(self, sheet_id: core_types.Id_, data: schema.UpdateCellSchema) -> None:
        await self.repo().update_cell(sheet_id, data)

    async def delete_rows(self, sheet_id: core_types.Id_, row_ids: list[core_types.Id_]) -> None:
        await self.repo().delete_rows(sheet_id, row_ids)
