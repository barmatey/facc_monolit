import pandas as pd
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Boolean, String

from .. import core_types
from ..sheet import entities
from .base import BaseRepo

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


class SheetRepo(BaseRepo):

    async def create(self, data: entities.SheetCreate) -> core_types.MongoId:
        id_ = str(pd.Timestamp.now().timestamp())
        return id_
