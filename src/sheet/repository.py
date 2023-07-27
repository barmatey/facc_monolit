from abc import ABC, abstractmethod

from src import core_types
from . import entities, schema


class SheetRepo(ABC):
    @abstractmethod
    async def create_one(self, data: schema.SheetCreateSchema) -> core_types.Id_:
        pass

    @abstractmethod
    async def get_one(self, data: schema.SheetRetrieveSchema) -> entities.Sheet:
        pass

    @abstractmethod
    async def get_scroll_size(self, sheet_id: core_types.Id_) -> entities.ScrollSize:
        pass

    @abstractmethod
    async def update_col_size(self, sheet_id: core_types.Id_, data: schema.UpdateSindexSizeSchema) -> None:
        pass

    @abstractmethod
    async def update_cell_one(self, sheet_id: core_types.Id_, data: entities.Cell) -> None:
        pass

    @abstractmethod
    async def update_cell_many(self, sheet_id: core_types.Id_, data: list[entities.Cell]) -> None:
        pass

    @abstractmethod
    async def delete_row_many(self, sheet_id: core_types.Id_, row_ids: list[core_types.Id_]) -> None:
        pass

    @abstractmethod
    async def get_col_filter(self, data: schema.ColFilterRetrieveSchema) -> entities.ColFilter:
        pass

    @abstractmethod
    async def update_col_filter(self, data: entities.ColFilter) -> None:
        pass

    @abstractmethod
    async def update_col_sorter(self, data: entities.ColSorter) -> None:
        pass

    @abstractmethod
    async def clear_all_filters(self, sheet_id: core_types.Id_) -> None:
        pass
