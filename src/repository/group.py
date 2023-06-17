import typing
from loguru import logger

from sqlalchemy import Table, Column, Integer, ForeignKey, String, MetaData

from ..report import entities as entities_report
from ..sheet import entities as entities_sheet
from .. import core_types
from . import db
from .base import BaseRepo
from .sheet import SheetCrudRepo
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
    sheet_repo = SheetCrudRepo

    async def create(self, data: entities_report.GroupCreate) -> core_types.Id_:
        async with db.get_async_session() as session:
            # Create sheet model
            sheet_data = entities_sheet.SheetCreate(
                df=data.dataframe,
                drop_index=data.drop_index,
                drop_columns=data.drop_columns,
            )
            sheet_id = await self.sheet_repo().create_with_session(sheet_data, session)

            # Create group
            # group_data = data.dict()
            # group_data['sheet_id'] = sheet_id
            # group_id = await super()._create(group_data, session, commit=False)

            # _ = await session.commit()
            return group_id

    async def retrieve(self, id_: core_types.Id_) -> entities_report.Group:
        async with db.get_async_session() as session:
            data = await super()._retrieve(id_, session)
            group = entities_report.Group(**data)
            return group

    async def delete(self, id_: core_types.Id_) -> None:
        async with db.get_async_session() as session:
            await super()._delete(id_, session, commit=True)
