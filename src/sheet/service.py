import pandas as pd

from  src import core_types

from . import schema, entities, events
from .repository import SheetRepo


class SheetService:

    def __init__(self, sheet_repo: SheetRepo):
        self.sheet_repo = sheet_repo

    async def create_one(self, data: events.SheetCreated) -> core_types.Id_:
        sheet_id = await self.sheet_repo.create_one(data)
        return sheet_id

    async def overwrite_one(self, sheet_id: core_types.Id_, data: events.SheetCreated) -> None:
        await self.sheet_repo.overwrite_one(sheet_id, data)

    async def get_one(self, data: schema.SheetRetrieveSchema) -> schema.SheetSchema:
        sheet_schema = await self.sheet_repo.get_one(data=data)
        return sheet_schema

    async def get_one_as_frame(self, data: events.SheetGotten) -> pd.DataFrame:
        sheet_df = await self.sheet_repo.get_one_as_frame(sheet_id=data.sheet_id)
        return sheet_df

    async def get_scroll_size(self, sheet_id: core_types.Id_) -> entities.ScrollSize:
        scroll_size = await self.sheet_repo.get_scroll_size(sheet_id=sheet_id)
        scroll_size = schema.ScrollSizeSchema.from_scroll_size_entity(scroll_size)
        return scroll_size

    async def get_col_filter(self, data: schema.ColFilterRetrieveSchema) -> schema.ColFilterSchema:
        col_filter = await self.sheet_repo.get_col_filter(data)
        return col_filter

    async def update_col_filter(self, data: schema.ColFilterSchema) -> None:
        await self.sheet_repo.update_col_filter(data)

    async def clear_all_filters(self, sheet_id: core_types.Id_) -> None:
        await self.sheet_repo.clear_all_filters(sheet_id)

    async def update_col_sorter(self, data: schema.ColSorterSchema) -> None:
        await self.sheet_repo.update_col_sorter(data)

    async def update_col_size(self, sheet_id: core_types.Id_, data: schema.UpdateSindexSizeSchema) -> None:
        await self.sheet_repo.update_col_size(sheet_id, data)

    async def update_cell(self, sheet_id: core_types.Id_, data: schema.PartialUpdateCellSchema) -> None:
        await self.sheet_repo.update_cell_one(sheet_id, data)

    async def update_cell_many(self, sheet_id: core_types.Id_, data: list[schema.PartialUpdateCellSchema]) -> None:
        await self.sheet_repo.update_cell_many(sheet_id, data)

    async def delete_row_many(self, sheet_id: core_types.Id_, row_ids: list[core_types.Id_]) -> None:
        await self.sheet_repo.delete_row_many(sheet_id, row_ids)
