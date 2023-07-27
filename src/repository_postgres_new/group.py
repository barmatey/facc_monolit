import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from src.report import entities as entities_report
from src.sheet import entities as entities_sheet
from src import core_types
from src import db

from repository_postgres.category import CategoryEnum
from repository_postgres.group import Group

from src.report.repository import GroupRepo

from .base import ReturningEntity
from .sheet import SheetRepo
from .base import BasePostgres


class GroupRepoPostgres(BasePostgres, GroupRepo):
    model = Group

    def __init__(self,
                 session: AsyncSession,
                 returning: ReturningEntity = "ENTITY",
                 fields: list[str] = None,
                 scalars: bool = False,
                 ):
        super().__init__(session, returning, fields, scalars)
        self.__sheet_repo = SheetRepo(session, returning="MODEL")

    async def create_one(self, data: entities_report.GroupCreate) -> entities_report.Group:
        # Create sheet model
        sheet_data = entities_sheet.SheetCreate(
            df=data.dataframe,
            drop_index=data.drop_index,
            drop_columns=data.drop_columns,
        )
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
