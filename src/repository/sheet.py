import numpy as np
from loguru import logger
from sqlalchemy import ForeignKey, Integer, Boolean, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from . import db
from .. import core_types
from ..sheet import entities
from .base import BaseRepo, BaseModel
from .service.normalizer import Normalizer


class Sheet(BaseModel):
    __tablename__ = "sheet"


class Row(BaseModel):
    __tablename__ = "sheet_row"
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False)


class Col(BaseModel):
    __tablename__ = "sheet_col"
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False)


class Cell(BaseModel):
    __tablename__ = "sheet_cell"
    value: Mapped[str] = mapped_column(String(1000), nullable=True)
    dtype: Mapped[str] = mapped_column(String(30), nullable=False)
    is_readonly: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_index: Mapped[bool] = mapped_column(Boolean, nullable=False)
    color: Mapped[str] = mapped_column(String(16), nullable=True)
    row_id: Mapped[int] = mapped_column(Integer, ForeignKey(Row.id, ondelete='CASCADE'), nullable=False)
    col_id: Mapped[int] = mapped_column(Integer, ForeignKey(Col.id, ondelete='CASCADE'), nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False)


class RowRepo(BaseRepo):
    model = Row


class ColRepo(BaseRepo):
    model = Col


class CellRepo(BaseRepo):
    model = Cell


class SheetRepo(BaseRepo):
    model = Sheet
    row_repo = RowRepo
    col_repo = ColRepo
    cell_repo = CellRepo
    normalizer = Normalizer

    async def create_with_session(self, session: AsyncSession, data: entities.SheetCreate) -> core_types.Id_:
        sheet_id = await super().create_with_session(session, {})

        # Create row, col and cell data from denormalized dataframe
        normalizer = self.normalizer(**data.dict())
        normalizer.normalize()

        # Create sindexes, save created ids for following create cells
        rows = normalizer.get_normalized_rows().assign(sheet_id=sheet_id).to_dict(orient='records')
        cols = normalizer.get_normalized_cols().assign(sheet_id=sheet_id).to_dict(orient='records')

        row_ids = await self.row_repo().create_bulk_with_session(session, rows)
        col_ids = await self.col_repo().create_bulk_with_session(session, cols)

        # Create cells
        repeated_row_ids = np.repeat(row_ids, len(col_ids))
        repeated_col_ids = col_ids * len(row_ids)
        cells = normalizer.get_normalized_cells().assign(
            sheet_id=sheet_id, row_id=repeated_row_ids, col_id=repeated_col_ids).to_dict(orient='records')
        _ = await self.cell_repo().create_bulk_with_session(session, cells)

        return sheet_id

    async def create(self, data: entities.SheetCreate) -> core_types.Id_:
        async with db.get_async_session() as session:
            sheet_id = await self.create_with_session(session, data)
            await session.commit()
            return sheet_id
