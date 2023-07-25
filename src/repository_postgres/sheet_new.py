import typing

import numpy as np
import pandas as pd
from sqlalchemy import ForeignKey, Integer, Boolean, String
from sqlalchemy.ext.asyncio import AsyncSession as AS
from sqlalchemy.orm import Mapped, mapped_column

from frepository import PostgresRepository
from repository_postgres import db
from repository_postgres.base import BaseModel
from repository_postgres.normalizer import Normalizer, Denormalizer
from src.sheet import entities, schema
from src.core_types import Id_


class Sheet(BaseModel):
    __tablename__ = "sheet"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


class Row(BaseModel):
    __tablename__ = "sheet_row"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_readonly: Mapped[int] = mapped_column(Boolean, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False, index=True)


class Col(BaseModel):
    __tablename__ = "sheet_col"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_readonly: Mapped[int] = mapped_column(Boolean, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False, index=True)


class Cell(BaseModel):
    __tablename__ = "sheet_cell"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    value: Mapped[str] = mapped_column(String(1000), nullable=True, index=True)
    dtype: Mapped[str] = mapped_column(String(30), nullable=False)
    is_readonly: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_index: Mapped[bool] = mapped_column(Boolean, nullable=False)
    color: Mapped[str] = mapped_column(String(16), nullable=True)
    text_align: Mapped[str] = mapped_column(String(8), default='left')
    row_id: Mapped[int] = mapped_column(Integer, ForeignKey(Row.id, ondelete='CASCADE'), nullable=False, index=True)
    col_id: Mapped[int] = mapped_column(Integer, ForeignKey(Col.id, ondelete='CASCADE'), nullable=False, index=True)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False, index=True)


class SindexScrollSize(typing.TypedDict):
    count_sindexes: int
    scroll_size: int


class SindexRepo(PostgresRepository):
    model = Row | Col

    async def s_get_scroll_size(self, session: AS, sheet_id: Id_):
        raise NotImplemented

    async def update_sindex_size(self, sheet_id: Id_, data: entities.UpdateSindexSize):
        raise NotImplemented

    async def delete_many(self, filter_by: dict) -> None:
        raise NotImplemented

    async def _update_scroll_pos_and_indexes_with_session(self, session: AS, sheet_id: Id_) -> None:
        # Get data
        filter_by = {"sheet_id": sheet_id}
        order_by = 'index'
        sindexes = await self.s_get_many_as_frame(session, filter_by, order_by)
        sindexes = sindexes.rename({"id": "sindex_id"}, axis=1)

        # Calculate new values
        sindexes.loc[~sindexes['is_filtred'] | sindexes['is_freeze'], 'size'] = 0
        sindexes['scroll_pos'] = sindexes['size'].cumsum().shift(1).fillna(0)
        sindexes.loc[sindexes['is_freeze'], 'scroll_pos'] = -1
        sindexes['index_value'] = range(0, len(sindexes.index))

        # Update data
        values: list[dict] = sindexes[['sindex_id', 'is_filtred', 'scroll_pos', 'index_value']].to_dict(
            orient='records')
        await super().s_update_many(session, values, search_keys=['sindex_id'])


class RowRepo(SindexRepo):
    model = Row


class ColRepo(SindexRepo):
    model = Col


class CellRepo(PostgresRepository):
    model = Cell


class SheetRepo(PostgresRepository):
    model = Sheet
    row_repo = RowRepo
    col_repo = ColRepo
    cell_repo = CellRepo
    normalizer = Normalizer
    denormalizer = Denormalizer
    session_maker = db.get_async_session

    async def s_create_one_return_entity(self, session, data: schema.SheetCreateSchema) -> entities.Sheet:
        sheet: Sheet = await super().s_create_one_return_model()
        await self._create_rows_cols_and_cells(session, sheet.id, data)
        return sheet.to_entity()

    async def _create_rows_cols_and_cells(self, session: AS, sheet_id: Id_, data: schema.SheetCreateSchema) -> None:
        # Create row, col and cell data from denormalized dataframe
        normalizer = self.normalizer(**data.dict())
        normalizer.normalize()

        # Create sindexes, save created ids for following create cells
        rows = normalizer.get_normalized_rows().assign(sheet_id=sheet_id).to_dict(orient='records')
        cols = normalizer.get_normalized_cols().assign(sheet_id=sheet_id).to_dict(orient='records')

        row_ids = await self.row_repo().s_create_many_return_fields(session, rows, fields=['id'], scalars=True)
        col_ids = await self.col_repo().s_create_many_return_fields(session, cols, fields=['id'], scalars=True)

        # Create cells
        repeated_row_ids = np.repeat(row_ids, len(col_ids))
        repeated_col_ids = col_ids * len(row_ids)
        cells = (normalizer.get_normalized_cells()
                 .assign(sheet_id=sheet_id, row_id=repeated_row_ids, col_id=repeated_col_ids)
                 .to_dict(orient='records')
                 )
        _ = await self.cell_repo().s_create_many(session, cells)

    async def get_one_as_sheet(self, data: schema.SheetRetrieveSchema) -> entities.Sheet:
        if data.from_scroll is None or data.to_scroll is None:
            return await self._get_as_sheet_without_pagination(data)
        return await self._get_as_sheet_with_pagination(data)

    async def _get_as_sheet_with_pagination(self, data: entities.SheetRetrieve) -> entities.Sheet:
        async with self.session_maker() as session:
            # Find rows
            filter_by = {}
            order_by = []
            rows: pd.DataFrame = await self.row_repo().s_get_many_as_frame(session, filter_by, order_by, asc=True)

            # Find cols
            filter_by = {"sheet_id": data.sheet_id}
            order_by = ["scroll_pos"]
            cols: pd.DataFrame = await self.col_repo().s_get_many_as_frame(session, filter_by, order_by)

            # Find cells
            filter_by = {}
            order_by = []
            cells: pd.DataFrame = await self.cell_repo().s_get_many_as_frame(session, filter_by, order_by)

            sheet = self._merge_into_sheet_entity(data.sheet_id, rows, cols, cells)
            return sheet

    async def _get_as_sheet_without_pagination(self, data: entities.SheetRetrieve) -> entities.Sheet:
        async with self.session_maker() as session:
            filter_by = {"sheet_id": data.sheet_id, "is_filtred": True, }
            order_by = 'index'
            rows = await self.row_repo().s_get_many_as_frame(session, filter_by, order_by)
            cols = await self.col_repo().s_get_many_as_frame(session, filter_by, order_by)
            cells = await self.cell_repo().s_get_many_as_frame(session, filter_by, order_by)
            sheet = self._merge_into_sheet_entity(data.sheet_id, rows, cols, cells)
            return sheet

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
