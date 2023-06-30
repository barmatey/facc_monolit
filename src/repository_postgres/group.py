import pandas as pd
from sqlalchemy import Integer, ForeignKey, String, JSON
from sqlalchemy.orm import Mapped, mapped_column

from report.enums import CategoryLiteral
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

    def to_group_entity(self) -> entities_report.Group:
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
                category_id=CategoryEnum[data.category].value,
                columns=data.columns,
                source_id=data.source_id,
                sheet_id=sheet_id,
            )
            group_id = await self.create_with_session(session, group_data)

            await session.commit()
            return group_id

    async def retrieve_bulk(self, filter_: dict = None, sort_by: str = None, ascending=True) -> list[entities_report.Group]:
        if filter_ is None:
            filter_ = {}

        async with db.get_async_session() as session:
            # noinspection PyTypeChecker
            groups: list[Group] = await super().retrieve_bulk_with_session(session, filter_)
            groups: list[entities_report.Group] = [g.to_group_entity() for g in groups]
            return groups

    async def retrieve_by_id(self, id_: core_types.Id_) -> entities_report.Group:
        # noinspection PyTypeChecker
        group: Group = await self.retrieve({"id": id_})
        group: entities_report.Group = group.to_group_entity()
        return group

    async def delete_by_id(self, id_: core_types.Id_) -> core_types.Id_:
        async with db.get_async_session() as session:
            # noinspection PyTypeChecker
            group: Group = await self.retrieve_and_delete_with_session(session, filter_={"id": id_})
            await self.sheet_repo().delete_with_session(session, filter_={"id": group.sheet_id})
            group_id = group.id
            await session.commit()
            return group_id

    async def retrieve_linked_sheet_as_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        async with db.get_async_session() as session:
            # noinspection PyTypeChecker
            group: Group = await self.retrieve_with_session(session, filter_={"id": group_id})
            df = await self.sheet_repo().retrieve_as_dataframe_with_session(session, group.sheet_id)
            return df
