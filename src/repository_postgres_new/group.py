import pandas as pd
from sqlalchemy import Integer, ForeignKey, String, JSON
from sqlalchemy.orm import Mapped, mapped_column

from report import entities
from src.report import entities as entities_report
from src.sheet import entities as entities_sheet
from src import core_types

from . import db
from .base import BaseRepo, BaseModel
from .sheet import Sheet, SheetRepo
from .category import Category, CategoryEnum
from .source import Source


class Group(BaseModel):
    __tablename__ = "group"
    title: Mapped[str] = mapped_column(String(30), nullable=False)
    columns: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey(Category.id, ondelete='CASCADE'), nullable=False)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey(Source.id, ondelete='CASCADE'), nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='RESTRICT'), nullable=False,
                                          unique=True)

    def to_entity(self) -> entities_report.Group:
        converted = entities_report.Group(
            id=self.id,
            title=self.title,
            category=CategoryEnum(self.category_id).name,
            sheet_id=self.sheet_id,
            source_id=self.source_id,
        )
        return converted


class GroupRepo(BaseRepo):
    model = Group
    sheet_repo = SheetRepo

    async def create(self, data: entities_report.GroupCreate) -> entities.Group:
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
                category_id=CategoryEnum[data.category].value,
                columns=data.columns,
                source_id=data.source_id,
                sheet_id=sheet_id,
            )
            group: Group = await super().create_with_session(session, group_data)
            entity = group.to_entity()
            await session.commit()
            return entity

    async def retrieve_linked_sheet_as_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        async with db.get_async_session() as session:
            group: Group = await self.retrieve_with_session(session, filter_by={"id": group_id})
            df = await self.sheet_repo().retrieve_as_dataframe_with_session(session, sheet_id=group.sheet_id)
            return df