from sqlalchemy import Table, Column, Integer, ForeignKey, String, MetaData

from .. import core_types, entities
from . import db
from .category import Category
from .source import SourceBase

metadata = MetaData()

Group = Table(
    'group',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String(80), nullable=False),
    Column("category_id", Integer, ForeignKey(Category.c.id, ondelete='CASCADE'), nullable=False),
    Column("source_id", Integer, ForeignKey(SourceBase.c.id, ondelete='CASCADE'), nullable=False),
    Column("sheet_id", String(30), nullable=False, unique=True),
)


class GroupRepo:
    table = Group

    async def create(self, data: entities.Group) -> core_types.Id_:
        data_dict = data.dict()
        async with db.get_async_session() as session:
            insert = self.table.insert().values(**data_dict).returning(self.table.c.id)
            result = await session.execute(insert)
            await session.commit()
            result = result.fetchone()[0]
            return result

    async def retrieve(self, id_: core_types.Id_) -> entities.Group:
        async with db.get_async_session() as session:
            select = self.table.select().where(self.table.c.id == id_)
            cursor = await session.execute(select)
            result = {col.key: value for col, value in zip(self.table.columns, cursor.fetchone())}
            result = entities.Group(
                id_=result['id'],
                title=result['title'],
                category_id='TEMP',
                sheet_id=result['sheet'],
                source_id=result['source_base'],
            )
            return result

    async def delete(self, id_: core_types.Id_) -> None:
        async with db.get_async_session() as session:
            delete = self.table.delete().where(self.table.c.id == id_)
            await session.execute(delete)
            await session.commit()
