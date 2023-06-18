import typing
from loguru import logger

from sqlalchemy import Table, Column, Integer, ForeignKey, String, JSON, MetaData, Result

from ..report import entities as entities_report
from ..sheet import entities as entities_sheet
from .. import core_types
from . import db
from .base import BaseRepo
from .sheet import Sheet, SheetRepo
from .category import Category
from .source import SourceBase

metadata = MetaData()

Group = Table(
    'group',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String(80), nullable=False),
    Column("columns", JSON, nullable=False),
    Column("category_id", Integer, ForeignKey(Category.c.id, ondelete='CASCADE'), nullable=False),
    Column("source_id", Integer, ForeignKey(SourceBase.c.id, ondelete='CASCADE'), nullable=False),
    Column("sheet_id", Integer, ForeignKey(Sheet.c.id, ondelete='RESTRICT'), nullable=False, unique=True),
)


class GroupRepo(BaseRepo):
    table = Group
    sheet_repo = SheetRepo

    async def create(self, data: entities_report.GroupCreate) -> core_types.Id_:
        async with db.get_async_session() as session:
            # Create sheet model
            sheet_data = entities_sheet.SheetCreate(
                df=data.dataframe,
                drop_index=data.drop_index,
                drop_columns=data.drop_columns,
            )
            sheet_id = await self.sheet_repo().create_with_session(sheet_data, session)

            # Create group model
            group_data = dict(
                title=data.title,
                category_id=data.category.value,
                columns=data.columns,
                source_id=data.source_id,
                sheet_id=sheet_id,
            )
            group_id = await self.create_with_session(group_data, session)

            _ = await session.commit()
            return group_id

    async def delete_by_id(self, id_: core_types.Id_) -> None:
        async with db.get_async_session() as session:
            group: dict = await self.retrieve_and_delete_with_session(session, id=id_)
            logger.debug(f"\n{group}, "
                         f"\ntype: {type(group)}")
