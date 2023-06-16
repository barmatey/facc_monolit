import random
from datetime import datetime

import pandas as pd
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Float
from sqlalchemy import String, JSON, TIMESTAMP

from .. import core_types, entities
from .base import BaseRepo

metadata = MetaData()

Sheet = Table(
    'sheet',
    metadata,
    Column("id", Integer, primary_key=True),
)

Row = Table(
    'row',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("sheet_id", Integer, ForeignKey(Sheet.c.id, ondelete='CASCADE')),

)

Col = Table(
    'col',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("sheet_id", Integer, ForeignKey(Sheet.c.id, ondelete='CASCADE')),

)

Cell = Table(
    'cell',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("sheet_id", Integer, ForeignKey(Sheet.c.id, ondelete='CASCADE')),
)


class SheetRepo(BaseRepo):

    async def create(self, data: entities.SheetCreateData) -> core_types.MongoId:
        id_ = str(pd.Timestamp.now().timestamp())
        return id_
