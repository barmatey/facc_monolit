import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

import core_types
from repository_postgres.normalizer import Normalizer, Denormalizer
from sheet import entities, schema
from src.sheet.repository import SheetRepo

from .base import BasePostgres, Model

from src.repository_postgres.sheet import Sheet as SheetModel
from src.repository_postgres.sheet import Row as RowModel
from src.repository_postgres.sheet import Col as ColModel
from src.repository_postgres.sheet import Cell as CellModel


class SheetSindex(BasePostgres):
    model: Model = NotImplemented

    async def create_many(self, data: list[core_types.DTO]) -> list[core_types.Id_]:
        session = self._session
        data = self._parse_dto(data)
        stmt = insert(self.model).returning(self.model.id)
        result = await session.execute(stmt, data)
        ids = list(result.scalars())
        return ids


class SheetRow(SheetSindex):
    model = RowModel


class SheetCol(SheetSindex):
    model = ColModel


class SheetCell(BasePostgres):
    model = CellModel

    async def create_many(self, data: list[core_types.DTO]) -> None:
        session = self._session
        data = self._parse_dto(data)
        stmt = insert(self.model).returning(self.model)
        _ = await session.execute(stmt, data)

    async def update_cell_one(self, sheet_id: core_types.Id_, data: entities.Cell) -> None:
        raise NotImplemented

    async def update_cell_many(self, sheet_id: core_types.Id_, data: list[entities.Cell]) -> None:
        raise NotImplemented


class SheetFilter(BasePostgres):
    model = NotImplemented

    async def get_col_filter(self, data: schema.ColFilterRetrieveSchema) -> entities.ColFilter:
        raise NotImplemented

    async def update_col_filter(self, data: entities.ColFilter) -> None:
        raise NotImplemented


class SheetSorter(BasePostgres):
    model = NotImplemented

    async def update_col_sorter(self, data: entities.ColSorter) -> None:
        raise NotImplemented


class SheetCrud(BasePostgres):
    model = SheetModel

    def __init__(self, session: AsyncSession):
        super().__init__(session)
        self.__sheet_cell = SheetCell(session)
        self.__sheet_row = SheetRow(session)
        self.__sheet_col = SheetCol(session)
        self.normalizer = Normalizer
        self.denormalizer = Denormalizer

    async def create_one(self, data: schema.SheetCreateSchema) -> core_types.Id_:
        sheet: SheetModel = await super().create_one({})
        await self._create_rows_cols_and_cells(sheet.id, data)
        return sheet.id

    async def get_one(self, data: schema.SheetRetrieveSchema) -> entities.Sheet:
        filter_by = {"sheet_id": data.sheet_id, "is_filtred": True, }
        order_by = 'index'
        rows = await self.__sheet_row.get_many_as_frame(filter_by, order_by)
        cols = await self.__sheet_col.get_many_as_frame(filter_by, order_by)
        cells = await self.__sheet_cell.get_many_as_frame(filter_by)
        return self._merge_into_sheet_entity(data.sheet_id, rows, cols, cells)

    async def _create_rows_cols_and_cells(self, sheet_id: core_types.Id_, data: entities.SheetCreate) -> None:
        # Create row, col and cell data from denormalized dataframe
        normalizer = self.normalizer(**data.dict())
        normalizer.normalize()

        # Create sindexes, save created ids for following create cells
        rows = normalizer.get_normalized_rows().assign(sheet_id=sheet_id).to_dict(orient='records')
        cols = normalizer.get_normalized_cols().assign(sheet_id=sheet_id).to_dict(orient='records')

        row_ids = await self.__sheet_row.create_many(rows)
        col_ids = await self.__sheet_col.create_many(cols)

        # Create cells
        repeated_row_ids = np.repeat(row_ids, len(col_ids))
        repeated_col_ids = col_ids * len(row_ids)
        cells = normalizer.get_normalized_cells().assign(
            sheet_id=sheet_id, row_id=repeated_row_ids, col_id=repeated_col_ids).to_dict(orient='records')
        _ = await self.__sheet_cell.create_many(cells)

    @staticmethod
    def _merge_into_sheet_entity(sheet_id, rows, cols, cells, ) -> entities.Sheet:
        saved_cols = cells.columns.copy()
        cells = pd.merge(cells, rows[['id', 'index', ]], left_on='row_id', right_on='id', suffixes=('', '_row'))
        cells = pd.merge(cells, cols[['id', 'index', ]], left_on='col_id', right_on='id', suffixes=('', '_col'))
        cells = cells.sort_values(['index', 'index_col'])[saved_cols]

        sheet = entities.Sheet(
            id=sheet_id,
            rows=rows.to_dict(orient='records'),
            cols=cols.to_dict(orient='records'),
            cells=cells.to_dict(orient='records'),
        )
        return sheet


class SheetRepoPostgres(SheetRepo):
    model = SheetModel

    def __init__(self, session: AsyncSession):
        self.__sheet_crud = SheetCrud(session)
        self.__sheet_cell = SheetCell(session)
        self.__sheet_row = SheetRow(session)
        self.__sheet_col = SheetCol(session)

    async def create_one(self, data: schema.SheetCreateSchema) -> core_types.Id_:
        return await self.__sheet_crud.create_one(data)

    async def get_one(self, data: schema.SheetRetrieveSchema) -> entities.Sheet:
        return await self.__sheet_crud.get_one(data)

    async def get_scroll_size(self, sheet_id: core_types.Id_) -> entities.ScrollSize:
        raise NotImplemented

    async def update_col_size(self, sheet_id: core_types.Id_, data: schema.UpdateSindexSizeSchema) -> None:
        raise NotImplemented

    async def update_cell_one(self, sheet_id: core_types.Id_, data: entities.Cell) -> None:
        raise NotImplemented

    async def update_cell_many(self, sheet_id: core_types.Id_, data: list[entities.Cell]) -> None:
        raise NotImplemented

    async def delete_row_many(self, sheet_id: core_types.Id_, row_ids: list[core_types.Id_]) -> None:
        raise NotImplemented

    async def get_col_filter(self, data: schema.ColFilterRetrieveSchema) -> entities.ColFilter:
        raise NotImplemented

    async def update_col_filter(self, data: entities.ColFilter) -> None:
        raise NotImplemented

    async def update_col_sorter(self, data: entities.ColSorter) -> None:
        raise NotImplemented

    async def clear_all_filters(self, sheet_id: core_types.Id_) -> None:
        raise NotImplemented
