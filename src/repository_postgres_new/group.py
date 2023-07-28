import pandas as pd
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from core_types import OrderBy
from report import entities
from report.entities import Group
from report.repository import Entity
from src.report import entities as entities_report
from src.sheet import entities as entities_sheet
from src import core_types
from src import db

from repository_postgres.category import CategoryEnum
from repository_postgres.group import Group as GroupModel

from src.report.repository import GroupRepo

from .base import ReturningEntity
from .sheet import SheetRepoPostgres
from .base import BasePostgres


class GroupRepoPostgres(BasePostgres, GroupRepo):
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
        groups = [x.to_entity()for x in models]
        return groups

    async def update_one(self, data: core_types.DTO, filter_by: dict) -> Group:
        raise NotImplemented

    async def delete_one(self, filter_by: dict) -> core_types.Id_:
        deleted_model = await super().delete_one(filter_by)
        return deleted_model.id

    async def overwrite_linked_sheet(self, instance: entities.Group, data: entities.SheetCreate) -> None:
        raise NotImplemented

    async def get_group_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        raise NotImplemented
