import pandas as pd

from core_types import DTO
from src import core_types

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

    async def get_full_sheet(self, data: events.SheetGotten) -> entities.Sheet:
        sheet_schema = await self.sheet_repo.get_full_sheet(data=data)
        return sheet_schema

    async def get_sheet_info(self, sheet_id: core_types.Id_) -> entities.SheetInfo:
        sheet_info = await self.sheet_repo.get_sheet_info(sheet_id)
        return sheet_info

    async def update_sheet_info(self, data: DTO, filter_by: dict) -> entities.SheetInfo:
        sheet_info = await self.sheet_repo.update_sheet_info(data, filter_by)
        return sheet_info

    async def get_one_as_frame(self, data: events.SheetGotten) -> pd.DataFrame:
        sheet_df = await self.sheet_repo.get_one_as_frame(sheet_id=data.sheet_id)
        return sheet_df

    async def get_col_filter(self, data: events.ColFilterGotten) -> entities.ColFilter:
        col_filter = await self.sheet_repo.get_col_filter(data)
        return col_filter

    async def update_col_filter(self, data: events.ColFilterUpdated) -> None:
        await self.sheet_repo.update_col_filter(data)

    async def clear_all_filters(self, sheet_id: core_types.Id_) -> None:
        await self.sheet_repo.clear_all_filters(sheet_id)

    async def update_col_sorter(self, data: events.ColSortedUpdated) -> None:
        await self.sheet_repo.update_col_sorter(data.col_sorter)

    async def update_col_size(self, data: events.ColWidthUpdated) -> None:
        await self.sheet_repo.update_col_size(data)

    async def update_cell(self, sheet_id: core_types.Id_, data: schema.PartialUpdateCellSchema) -> None:
        await self.sheet_repo.update_cell_one(sheet_id, data)

    async def update_cell_many(self, sheet_id: core_types.Id_, data: list[schema.PartialUpdateCellSchema]) -> None:
        await self.sheet_repo.update_cell_many(sheet_id,  data)

    async def delete_row_many(self, sheet_id: core_types.Id_, row_ids: list[core_types.Id_]) -> None:
        await self.sheet_repo.delete_row_many(sheet_id, row_ids)
