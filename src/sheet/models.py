from datetime import datetime
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Float
from sqlalchemy import String, JSON, TIMESTAMP

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
    Column("sheet_id", Integer, ForeignKey("sheet.id", ondelete='CASCADE')),

)

Col = Table(
    'col',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("sheet_id", Integer, ForeignKey("sheet.id", ondelete='CASCADE')),

)

Cell = Table(
    'cell',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("sheet_id", Integer, ForeignKey("sheet.id", ondelete='CASCADE')),
)
