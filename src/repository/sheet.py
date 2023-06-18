import numpy as np
from loguru import logger
import pandas as pd
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Boolean, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .. import core_types
from ..sheet import entities
from .base import BaseRepo, BaseModel
from .service.normalizer import Normalizer


class Sheet(BaseModel):
    __tablename__ = "sheet"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


class Row(BaseModel):
    __tablename__ = "sheet_row"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False)


class Col(BaseModel):
    __tablename__ = "sheet_col"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False)


class Cell(BaseModel):
    __tablename__ = "sheet_cell"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    value: Mapped[str] = mapped_column(String(1000), nullable=True)
    dtype: Mapped[str] = mapped_column(String(30), nullable=False)
    is_readonly: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_index: Mapped[bool] = mapped_column(Boolean, nullable=False)
    color: Mapped[str] = mapped_column(String(16), nullable=True)
    row_id: Mapped[int] = mapped_column(Integer, ForeignKey(Row.id, ondelete='CASCADE'), nullable=False)
    col_id: Mapped[int] = mapped_column(Integer, ForeignKey(Col.id, ondelete='CASCADE'), nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False)


class SindexRepo(BaseRepo):

    async def create_bulk_with_session(self, data: list[entities.SindexCreate], session: AsyncSession,
                                       chunksize=10_000) -> list[core_types.Id_]:
        sindex_ids = await super().create_bulk_with_session(data, session, chunksize)
        return sindex_ids


class RowRepo(SindexRepo):
    table = Row


class ColRepo(SindexRepo):
    table = Col


class CellRepo(BaseRepo):
    table = Cell


class SheetRepo(BaseRepo):
    table = Sheet
    row_repo = RowRepo
    col_repo = ColRepo
    cell_repo = CellRepo
    normalizer = Normalizer

    async def create_with_session(self, data: entities.SheetCreate, session: AsyncSession) -> core_types.Id_:
        sheet_id = await super().create_with_session({}, session)

        # Create sheet from denormalized dataframe
        normalizer = self.normalizer(**data.dict())
        normalizer.normalize()

        # Create sindexes, save created ids for following create cells
        rows = normalizer.get_normalized_rows().assign(sheet_id=sheet_id).to_dict(orient='records')
        cols = normalizer.get_normalized_cols().assign(sheet_id=sheet_id).to_dict(orient='records')

        row_ids = await self.row_repo().create_bulk_with_session(rows, session)
        col_ids = await self.col_repo().create_bulk_with_session(cols, session)

        # Create cells
        repeated_row_ids = np.repeat(row_ids, len(col_ids))
        repeated_col_ids = col_ids * len(row_ids)
        cells = normalizer.get_normalized_cells().assign(
            sheet_id=sheet_id, row_id=repeated_row_ids, col_id=repeated_col_ids).to_dict(orient='records')
        _ = await self.cell_repo().create_bulk_with_session(cells, session)

        return sheet_id

    async def create(self, data: entities.SheetCreate) -> core_types.Id_:
        normalizer = self.normalizer(**data.dict())
        normalizer.normalize()
        rows = normalizer.get_normalized_rows()
        cols = normalizer.get_normalized_cols()
        cells = normalizer.get_normalized_cells()

        logger.debug(f"\n{cells[0:3]}")

        return 321
