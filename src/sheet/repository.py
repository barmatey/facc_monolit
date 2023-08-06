from abc import ABC, abstractmethod

import pandas as pd

from core_types import DTO
from src import core_types
from . import entities, schema, events


class SheetRepo(ABC):
    @abstractmethod
    async def create_one(self, data: events.SheetCreated) -> core_types.Id_:
        raise NotImplemented

    @abstractmethod
    async def get_sheet_info(self, sheet_id: core_types.Id_) -> entities.SheetInfo:
        raise NotImplemented

    @abstractmethod
    async def update_sheet_info(self, data: DTO, filter_by: dict) -> entities.SheetInfo:
        raise NotImplemented

    @abstractmethod
    async def get_full_sheet(self, data: events.SheetGotten) -> entities.Sheet:
        raise NotImplemented

    @abstractmethod
    async def get_one_as_frame(self, sheet_id: core_types.Id_) -> pd.DataFrame:
        raise NotImplemented

    @abstractmethod
    async def overwrite_one(self, sheet_id: core_types.Id_, data: events.SheetCreated) -> None:
        raise NotImplemented

    @abstractmethod
    async def delete_one(self, filter_by: dict) -> None:
        raise NotImplemented

    @abstractmethod
    async def delete_many(self, filter_by: dict) -> None:
        raise NotImplemented

    @abstractmethod
    async def get_scroll_size(self, sheet_id: core_types.Id_) -> entities.ScrollSize:
        raise NotImplemented

    @abstractmethod
    async def update_col_size(self, data: events.ColWidthUpdated) -> None:
        raise NotImplemented

    @abstractmethod
    async def update_cell_one(self, sheet_id: core_types.Id_, data: schema.PartialUpdateCellSchema) -> None:
        raise NotImplemented

    @abstractmethod
    async def update_cell_many(self, sheet_id: core_types.Id_, data: list[schema.PartialUpdateCellSchema]) -> None:
        raise NotImplemented

    @abstractmethod
    async def delete_row_many(self, sheet_id: core_types.Id_, row_ids: list[core_types.Id_]) -> None:
        raise NotImplemented

    @abstractmethod
    async def get_col_filter(self, data: events.ColFilterGotten) -> entities.ColFilter:
        raise NotImplemented

    @abstractmethod
    async def update_col_filter(self, data: events.ColFilterUpdated) -> None:
        raise NotImplemented

    @abstractmethod
    async def update_col_sorter(self, data: entities.ColSorter) -> None:
        raise NotImplemented

    @abstractmethod
    async def clear_all_filters(self, sheet_id: core_types.Id_) -> None:
        raise NotImplemented
