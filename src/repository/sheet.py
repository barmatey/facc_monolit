import numpy as np
from loguru import logger
import pandas as pd
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Boolean, String
from sqlalchemy.ext.asyncio import AsyncSession

from .. import core_types
from ..sheet import entities
from .base import BaseRepo
from .service.normalizer import Normalizer

metadata = MetaData()

Sheet = Table(
    'sheet',
    metadata,
    Column("id", Integer, primary_key=True),
)

Row = Table(
    'sheet_row',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("size", Integer, nullable=False),
    Column("is_freeze", Boolean, nullable=False),
    Column("is_filtred", Boolean, nullable=False),
    Column("index", Integer, nullable=False),
    Column("scroll_pos", Integer, nullable=False),
    Column("sheet_id", Integer, ForeignKey(Sheet.c.id, ondelete='CASCADE'), nullable=False),
)

Col = Table(
    'sheet_col',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("size", Integer, nullable=False),
    Column("is_freeze", Boolean, nullable=False),
    Column("is_filtred", Boolean, nullable=False),
    Column("index", Integer, nullable=False),
    Column("scroll_pos", Integer, nullable=False),
    Column("sheet_id", Integer, ForeignKey(Sheet.c.id, ondelete='CASCADE'), nullable=False),
)

Cell = Table(
    'sheet_cell',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("value", String(1000), nullable=True),
    Column("dtype", String(30), nullable=False),
    Column("is_readonly", Boolean, nullable=False),
    Column("is_filtred", Boolean, nullable=False),
    Column("sheet_id", Integer, ForeignKey(Sheet.c.id, ondelete='CASCADE'), nullable=False),
)


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


class SheetCrudRepo(BaseRepo):
    row_repo = RowRepo
    col_repo = ColRepo
    cell_repo = CellRepo
    normalizer = Normalizer

    async def create_with_session(self, data: entities.SheetCreate, session: AsyncSession):
        normalizer = self.normalizer(**data.dict())
        normalizer.normalize()
        rows = normalizer.get_normalized_rows()
        cols = normalizer.get_normalized_cols()
        cells = normalizer.get_normalized_cells()

        row_ids = await self.row_repo().create_bulk_with_session(rows, session)
        logger.debug(f"{row_ids}")

    async def create(self, data: entities.SheetCreate) -> core_types.Id_:
        normalizer = self.normalizer(**data.dict())
        normalizer.normalize()
        rows = normalizer.get_normalized_rows()
        cols = normalizer.get_normalized_cols()
        cells = normalizer.get_normalized_cells()

        logger.debug(f"\n{cells[0:3]}")

        return 321
