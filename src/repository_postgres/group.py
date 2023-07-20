import pandas as pd
from sqlalchemy import Integer, ForeignKey, String, JSON
from sqlalchemy.orm import Mapped, mapped_column

from src.report import schema as schema_report
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
    fixed_columns: Mapped[list[str]] = mapped_column(JSON, nullable=False)
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
            columns=list(self.columns),
            fixed_columns=list(self.fixed_columns)
        )
        return converted


class GroupRepo(BaseRepo):
    model = Group
    sheet_repo = SheetRepo

    async def create(self, data: entities_report.GroupCreate) -> entities_report.Group:
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
                fixed_columns=data.fixed_columns,
                source_id=data.source_id,
                sheet_id=sheet_id,
            )
            group: Group = await super().create_with_session(session, group_data)
            entity = group.to_entity()
            await session.commit()
            return entity

    async def overwrite_linked_sheet(self, instance: entities_report.Group, data: entities_report.SheetCreate) -> None:
        async with db.get_async_session() as session:
            sheet_data = entities_sheet.SheetCreate(
                df=data.dataframe,
                drop_index=data.drop_index,
                drop_columns=data.drop_columns,
                readonly_all_cells=data.readonly_all_cells
            )
            await self.sheet_repo().overwrite_with_session(session, instance.sheet_id, sheet_data)
            await session.commit()

    async def delete(self, filter_by: dict) -> core_types.Id_:
        async with db.get_async_session() as session:
            group: Group = await self.retrieve_with_session(session, filter_by)
            await super().delete_with_session(session, filter_by)
            await self.sheet_repo().delete_with_session(session, filter_by={"id": group.sheet_id})
            session.expunge(group)
            await session.commit()
            return group.id

    async def retrieve_linked_sheet_as_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        async with db.get_async_session() as session:
            group: Group = await self.retrieve_with_session(session, filter_by={"id": group_id})
            df = await self.sheet_repo().retrieve_as_dataframe_with_session(session, sheet_id=group.sheet_id)
            return df

    async def delete_linked_sheet(self, group_id: core_types.Id_):
        async with db.get_async_session() as session:
            group: Group = await self.retrieve_with_session(session, filter_by={"id": group_id})
            await self.sheet_repo().delete_with_session(session, filter_by={"id": group.sheet_id})
            await session.commit()
