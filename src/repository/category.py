from sqlalchemy import Table, Column, Integer, String, MetaData

metadata = MetaData()

Category = Table(
    'category',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("value", String(80), nullable=False, unique=True),
)
