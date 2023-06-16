import typing

from sqlalchemy import Table, Column, Integer, ForeignKey, String, MetaData

from ..report import entities as entities_report
from ..sheet import entities as entities_sheet
from .. import core_types
from . import db
from .base import BaseRepo
from .sheet import SheetRepo
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

    async def create(self, data: entities_report.GroupCreateData) -> core_types.Id_:
        data = data.copy()
        async with db.get_async_session() as session:
            data.sheet_id = await self._create_sheet()
            group_id = await super()._create(data.dict(), session, commit=True)
            return group_id

    @staticmethod
    async def _create_sheet(sheet_repo: typing.Type[SheetRepo] = SheetRepo) -> core_types.MongoId:
        data = entities_sheet.SheetCreateData()
        return await sheet_repo().create(data)

    async def retrieve(self, id_: core_types.Id_) -> entities_report.Group:
        async with db.get_async_session() as session:
            data = await super()._retrieve(id_, session)
            group = entities_report.Group(**data)
            return group

    async def delete(self, id_: core_types.Id_) -> None:
        async with db.get_async_session() as session:
            await super()._delete(id_, session, commit=True)
