from datetime import datetime
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Float
from sqlalchemy import String, JSON, TIMESTAMP


metadata = MetaData()

Row = Table(
    'row',
    metadata,
    Column("id", Integer, primary_key=True),

)

Col = Table(
    'col',
    metadata,
    Column("id", Integer, primary_key=True),

)

Cell = Table(
    'cell',
    metadata,
    Column("id", Integer, primary_key=True),

)


