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

    @abstractmethod
    async def update_col_sorter(self, data: entities.ColSorter) -> None:
        pass

    @abstractmethod
    async def copy_rows(self, sheet_id: core_types.Id_,
                        copy_from: list[entities.CopySindex],
                        copy_to: list[entities.CopySindex]) -> None:
        pass

    @abstractmethod
    async def copy_cols(self, sheet_id: core_types.Id_,
                        copy_from: list[entities.CopySindex],
                        copy_to: list[entities.CopySindex]) -> None:
        pass

    @abstractmethod
    async def copy_cells(self, sheet_id: core_types.Id_, copy_from: list[entities.CopyCell],
                         copy_to: list[entities.CopyCell]) -> None:
        pass

    @abstractmethod
    async def update_col_size(self, sheet_id: core_types.Id_, data: entities.UpdateSindexSize) -> None:
        pass

    @abstractmethod
    async def update_cell(self, sheet_id: core_types.Id_, data: entities.UpdateCell) -> None:
        pass

    @abstractmethod
    async def delete_rows(self, sheet_id: core_types.Id_, row_ids: list[core_types.Id_]) -> None:
        pass


class PostgresRepo(Repository):
    sheet_repo = repository_postgres.SheetRepo
    sheet_row_repo = repository_postgres.RowRepo
    sheet_col_repo = repository_postgres.ColRepo
    sheet_cell_repo = repository_postgres.CellRepo
    sheet_table_repo = repository_postgres.SheetTableRepo
    sheet_filter_repo = repository_postgres.SheetFilterRepo
    sheet_sorter_repo = repository_postgres.SheetSorterRepo

    async def retrieve_sheet(self, data: entities.SheetRetrieve) -> entities.Sheet:
        return await self.sheet_repo().retrieve_as_sheet(data=data)

    async def retrieve_scroll_size(self, sheet_id: core_types.Id_) -> entities.ScrollSize:
        return await self.sheet_repo().retrieve_scroll_size(id_=sheet_id)

    async def retrieve_col_filter(self, data: entities.ColFilterRetrieve) -> entities.ColFilter:
        return await self.sheet_filter_repo().retrieve_col_filter(sheet_id=data['sheet_id'], col_id=data['col_id'])

    async def update_col_filter(self, data: entities.ColFilter) -> None:
        await self.sheet_filter_repo().update_col_filter(data)

    async def update_col_sorter(self, data: entities.ColSorter) -> None:
        await self.sheet_sorter_repo().update_col_sorter(data)

    async def copy_rows(self, sheet_id: core_types.Id_,
                        copy_from: list[entities.CopySindex],
                        copy_to: list[entities.CopySindex]) -> None:
        await self.sheet_table_repo().copy_rows(sheet_id, copy_from, copy_to)

    async def copy_cols(self, sheet_id: core_types.Id_,
                        copy_from: list[entities.CopySindex],
                        copy_to: list[entities.CopySindex]) -> None:
        await self.sheet_table_repo().copy_cols(sheet_id, copy_from, copy_to)

    async def copy_cells(self, sheet_id: core_types.Id_,
                         copy_from: list[entities.CopyCell], copy_to: list[entities.CopyCell]) -> None:
        await self.sheet_table_repo().copy_cells(sheet_id, copy_from, copy_to)

    async def update_col_size(self, sheet_id: core_types.Id_, data: entities.UpdateSindexSize) -> None:
        await self.sheet_col_repo().update_sindex_size(sheet_id, data)

    async def update_cell(self, sheet_id: core_types.Id_, data: entities.UpdateCell) -> None:
        await self.sheet_cell_repo().update(sheet_id, data)

    async def delete_rows(self, sheet_id: core_types.Id_, row_ids: list[core_types.Id_]) -> None:
        await self.sheet_row_repo().delete_bulk(sheet_id, row_ids)
