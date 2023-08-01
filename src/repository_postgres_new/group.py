import pandas as pd
from sqlalchemy import String, JSON, Integer, ForeignKey
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import mapped_column, Mapped

from src.core_types import OrderBy
from src.report.entities import Group
from src.report import entities as entities_report
from src.sheet import entities as entities_sheet
from src import core_types

from src.report.repository import GroupRepo

from .category import CategoryModel, CategoryEnum
from .sheet import SheetRepoPostgres, SheetModel
from .base import BasePostgres, BaseModel
from .source import SourceModel

from src.group.repository import GroupRepository


class GroupModel(BaseModel):
    __tablename__ = "group"
    title: Mapped[str] = mapped_column(String(30), nullable=False)
    columns: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    fixed_columns: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey(CategoryModel.id, ondelete='CASCADE'), nullable=False)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey(SourceModel.id, ondelete='CASCADE'), nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(SheetModel.id, ondelete='RESTRICT'), nullable=False,
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


class GroupRepoPostgres(BasePostgres, GroupRepo, GroupRepository):
    model = GroupModel

    def __init__(self, session: AsyncSession):
        super().__init__(session)
        self.__sheet_repo = SheetRepoPostgres(session)

    async def create_one(self, data: entities_report.GroupCreate) -> Group:
        # Create sheet model
        sheet_data = entities_sheet.SheetCreate(
            df=data.dataframe,
            drop_index=data.drop_index,
            drop_columns=data.drop_columns,
        )
        sheet_id = await self.__sheet_repo.create_one(sheet_data)

        # Create group model
        group_data = dict(
            title=data.title,
            category_id=CategoryEnum[data.category].value,
            columns=data.columns,
            fixed_columns=data.fixed_columns,
            source_id=data.source_id,
            sheet_id=sheet_id,
        )
        group_model = await super().create_one(group_data)
        return group_model.to_entity()

    async def get_one(self, filter_by: dict) -> Group:
        model = await super().get_one(filter_by)
        group = model.to_entity()
        return group

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True, slice_from: int = None,
                       slice_to: int = None) -> list[Group]:
        models = await super().get_many(filter_by, order_by, asc, slice_from, slice_to)
        groups = [x.to_entity() for x in models]
        return groups

    async def update_one(self, data: core_types.DTO, filter_by: dict) -> Group:
        model = await super().update_one(data, filter_by)
        return model.to_entity()

    async def delete_one(self, filter_by: dict) -> core_types.Id_:
        deleted_model: GroupModel = await super().delete_one(filter_by)
        await self.__sheet_repo.delete_one({"id": deleted_model.sheet_id})
        return deleted_model.id

    async def overwrite_linked_sheet(self, instance: entities_report.Group, data: entities_report.SheetCreate) -> None:
        sheet_create_data = entities_sheet.SheetCreate(
            df=data.dataframe,
            drop_index=data.drop_index,
            drop_columns=data.drop_columns,
        )
        await self.__sheet_repo.overwrite_one(instance.sheet_id, sheet_create_data)

    async def get_linked_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        group: GroupModel = await super().get_one({"id": group_id})
        df: pd.DataFrame = await self.__sheet_repo.get_one_as_frame(sheet_id=group.sheet_id)
        return df
