from sqlalchemy import Table, Column, Integer, ForeignKey, String, MetaData

from .. import core_types, entities
from . import db
from .base import BaseRepo
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


class GroupRepo(BaseRepo):
    table = Group

    async def create(self, data: entities.Group) -> core_types.Id_:
        async with db.get_async_session() as session:
            group_id = await super()._create(data.dict(), session, commit=True)
            return group_id

    async def retrieve(self, id_: core_types.Id_) -> entities.Group:
        async with db.get_async_session() as session:
            data = await super()._retrieve(id_, session)
            group = entities.Group(**data)
            return group

    async def delete(self, id_: core_types.Id_) -> None:
        async with db.get_async_session() as session:
            await super()._delete(id_, session, commit=True)
