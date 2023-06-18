from loguru import logger

from sqlalchemy import Integer, ForeignKey, String, JSON
from sqlalchemy.orm import Mapped, mapped_column

from ..report import entities as entities_report
from ..sheet import entities as entities_sheet
from .. import core_types
from . import db
from .base import BaseRepo, BaseModel
from .sheet import Sheet, SheetRepo
from .category import Category
from .source import Source


class Group(BaseModel):
    __tablename__ = "group"
    title: Mapped[str] = mapped_column(String(30), nullable=False)
    columns: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey(Category.id, ondelete='CASCADE'), nullable=False)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey(Source.id, ondelete='CASCADE'), nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='RESTRICT'), nullable=False,
                                          unique=True)


class GroupRepo(BaseRepo):
    model = Group
    sheet_repo = SheetRepo

    async def create(self, data: entities_report.GroupCreate) -> core_types.Id_:
        async with db.get_async_session() as session:
            # Create sheet model
            sheet_data = entities_sheet.SheetCreate(
                df=data.dataframe,
                drop_index=data.drop_index,
                drop_columns=data.drop_columns,
            )
            sheet_id = await self.sheet_repo().create_with_session(session, sheet_data)

            # Create group model
            group_data = dict(
                title=data.title,
                category_id=data.category.value,
                columns=data.columns,
                source_id=data.source_id,
                sheet_id=sheet_id,
            )
            group_id = await self.create_with_session(session, group_data)

            await session.commit()
            return group_id

    async def delete_by_id(self, id_: core_types.Id_) -> None:
        async with db.get_async_session() as session:
            group: Group = await self.retrieve_and_delete_with_session(session, filter_={"id": id_})
            await self.sheet_repo().delete_with_session(session, filter_={"id": group.sheet_id})
            await session.commit()
