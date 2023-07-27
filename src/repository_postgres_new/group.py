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
from .sheet import SheetRepo
from .base import BasePostgres


class GroupRepoPostgres(BasePostgres, GroupRepo):
    model = GroupModel

    def __init__(self, session: AsyncSession):
        super().__init__(session)
        self.__sheet_repo = SheetRepo(session)

    async def create_one(self, data: entities_report.GroupCreate) -> Group:
        # Create sheet model
        sheet_data = entities_sheet.SheetCreate(
            df=data.dataframe,
            drop_index=data.drop_index,
            drop_columns=data.drop_columns,
        )
        logger.debug(self.__sheet_repo.__dict__)
        sheet = await self.__sheet_repo.create_one(sheet_data)

        # Create group model
        group_data = dict(
            title=data.title,
            category_id=CategoryEnum[data.category].value,
            columns=data.columns,
            fixed_columns=data.fixed_columns,
            source_id=data.source_id,
            sheet_id=sheet.id,
        )
        group_entity = await super().create_one(group_data)
        return group_entity

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
        raise NotImplemented

    async def overwrite_linked_sheet(self, instance: entities.Group, data: entities.SheetCreate) -> None:
        raise NotImplemented

    async def get_group_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        raise NotImplemented


class GroupRepoPostgresOld(BasePostgres, GroupRepo):
    model = GroupModel

    def __init__(self, session: AsyncSession, ):
        super().__init__(session)
        self.__sheet_repo = SheetRepo(session)

    async def create_one(self, data: entities_report.GroupCreate) -> entities_report.Group:
        raise NotImplemented

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

    async def update_one(self, data: core_types.DTO, filter_by: dict) -> entities_report.Group:
        raise NotImplemented

    async def delete_one(self, filter_by: dict) -> core_types.Id_:
        async with db.get_async_session() as session:
            group: GroupModel = await self.retrieve_with_session(session, filter_by)
            await super().delete_with_session(session, filter_by)
            await self.sheet_repo().delete_with_session(session, filter_by={"id": group.sheet_id})
            session.expunge(group)
            await session.commit()
            return group.id

    async def retrieve_linked_sheet_as_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        async with db.get_async_session() as session:
            group: GroupModel = await self.retrieve_with_session(session, filter_by={"id": group_id})
            df = await self.sheet_repo().retrieve_as_dataframe_with_session(session, sheet_id=group.sheet_id)
            return df

    async def delete_linked_sheet(self, group_id: core_types.Id_):
        async with db.get_async_session() as session:
            group: GroupModel = await self.retrieve_with_session(session, filter_by={"id": group_id})
            await self.sheet_repo().delete_with_session(session, filter_by={"id": group.sheet_id})
            await session.commit()

    async def get_group_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        raise NotImplemented
