import pandas as pd
from sqlalchemy import String, JSON, Integer, ForeignKey, select, TIMESTAMP, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import mapped_column, Mapped

from src.core_types import OrderBy
from src.group.entities import Group, InnerSource, InnerSheet, InnerCategory
from src.group import events
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
    updated_at: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP(timezone=True), default=func.now(), onupdate=func.now())

    def to_entity(self, category: InnerCategory, sheet: InnerSheet, source: InnerSource) -> Group:
        converted = Group(
            id=self.id,
            title=self.title,
            category=category,
            sheet=sheet,
            source=source,
            ccols=list(self.columns),
            fixed_columns=list(self.fixed_columns),
            updated_at=self.updated_at,
        )
        return converted


class GroupRepoPostgres(BasePostgres, GroupRepo, GroupRepository):
    model = GroupModel

    def __init__(self, session: AsyncSession):
        super().__init__(session)
        self.__sheet_repo = SheetRepoPostgres(session)

    async def create_one(self, data: events.GroupCreated) -> Group:
        data = data.dict()
        data['category_id'] = CategoryEnum[data.pop('category')].value
        group_model: GroupModel = await super().create_one(data)
        return await self.get_one({"id":  group_model.id})

    async def get_one(self, filter_by: dict) -> Group:
        reports = await self.get_many(filter_by)
        if len(reports) != 1:
            raise LookupError
        return reports[0]

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True, slice_from: int = None,
                       slice_to: int = None) -> list[Group]:
        session = self._session
        filters = self._parse_filters(filter_by)
        stmt = (
            select(GroupModel, CategoryModel, SourceModel, SheetModel)
            .join(CategoryModel, GroupModel.category_id == CategoryModel.id)
            .join(SourceModel, GroupModel.source_id == SourceModel.id)
            .join(SheetModel, GroupModel.sheet_id == SheetModel.id)
            .where(*filters)
        )
        result = await session.execute(stmt)
        result = result.fetchall()
        result = [
            x[0].to_entity(
                category=x[1].to_entity(),
                source=InnerSource(id=x[2].id, title=x[2].title, updated_at=x[2].updated_at),
                sheet=InnerSheet(id=x[3].id, updated_at=x[3].updated_at),
            )
            for x in result
        ]
        return result

    async def update_one(self, data: core_types.DTO, filter_by: dict) -> Group:
        model = await super().update_one(data, filter_by)
        return await self.get_one(filter_by={"id": model.id})

    async def delete_one(self, filter_by: dict) -> core_types.Id_:
        deleted_model: GroupModel = await super().delete_one(filter_by)
        await self.__sheet_repo.delete_one({"id": deleted_model.sheet_id})
        return deleted_model.id

    async def get_linked_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        group: GroupModel = await super().get_one({"id": group_id})
        df: pd.DataFrame = await self.__sheet_repo.get_one_as_frame(sheet_id=group.sheet_id)
        return df
