import typing

import pandas as pd
from pydantic import BaseModel

from src import core_types
from .service_finrep import get_finrep_service
from . import repository
from . import entities, schema

OrderBy = typing.Union[str, list[str]]
DTO = typing.Union[BaseModel]


class Service:

    def __init__(self, repo: repository.CrudRepo):
        self.repo = repo

    async def create_one(self, data: BaseModel) -> entities.Entity:
        return await self.repo.create_one(data)

    async def get_one(self, filter_by: dict) -> entities.Entity:
        return await self.repo.get_one(filter_by)

    async def get_many(self, filter_by: dict, order_by: OrderBy = None) -> list[entities.Entity]:
        return await self.repo.get_many(filter_by, order_by)

    async def update_one(self, data: BaseModel, filter_by: dict) -> entities.Entity:
        return await self.repo.update_one(data, filter_by)

    async def delete_one(self, filter_by: dict) -> core_types.Id_:
        return await self.repo.delete_one(filter_by)


class GroupService(Service):

    def __init__(self, group_repo: repository.GroupRepo, wire_repo: repository.WireRepo):
        super().__init__(group_repo)
        self.group_repo = group_repo
        self.wire_repo = wire_repo

    async def create_one(self, data: schema.GroupCreateSchema) -> entities.Group:
        wire_df = await self.wire_repo.get_wire_dataframe(filter_by={"source_id": data.source_id})
        group_df: pd.DataFrame = await get_finrep_service(data.category).create_group(wire_df, data.columns)

        group_create = entities.GroupCreate(
            title=data.title,
            source_id=data.source_id,
            columns=data.columns,
            fixed_columns=data.fixed_columns,
            dataframe=group_df,
            drop_index=True,
            drop_columns=False,
            category=data.category,
        )
        group: entities.Group = await self.group_repo.create_one(group_create)
        return group

    async def total_recalculate(self, instance: entities.Group, wire_df: pd.DataFrame) -> entities.ExpandedGroup:
        old_group_df = await self.group_repo.get_linked_dataframe(instance.id)
        new_group_df = await get_finrep_service(instance.category).create_group(wire_df, instance.columns)

        if len(instance.fixed_columns):
            new_group_df = pd.merge(
                old_group_df[instance.fixed_columns],
                new_group_df,
                on=instance.fixed_columns, how='left'
            )

        data = entities.SheetCreate(
            dataframe=new_group_df,
            drop_index=True,
            drop_columns=False,
            readonly_all_cells=False
        )
        await self.group_repo.overwrite_linked_sheet(instance, data)
        expanded_group = entities.ExpandedGroup(**instance.dict(), sheet_df=new_group_df)
        return expanded_group


class ReportService(Service):

    def __init__(self, report_repo: repository.ReportRepo,
                 wire_repo: repository.WireRepo, group_repo: repository.GroupRepo):
        super().__init__(report_repo)
        self.wire_repo = wire_repo
        self.group_repo = group_repo

    async def create_one(self, data: schema.ReportCreateSchema) -> entities.Report:
        wire_df = await self.wire_repo.get_wire_dataframe(filter_by={"source_id": data.source_id})
        group_df = await self.group_repo.get_linked_dataframe(group_id=data.group_id)

        report_df = await get_finrep_service(data.category).create_report(wire_df, group_df, data.interval)
        sheet = entities.SheetCreate(dataframe=report_df, drop_index=False, drop_columns=False, readonly_all_cells=True)
        report_create = entities.ReportCreate(
            title=data.title,
            category=data.category,
            source_id=data.source_id,
            group_id=data.group_id,
            interval=data.interval,
            sheet=sheet,
        )
        report = await self.repo.create_one(report_create)
        return report

    async def total_recalculate(self, instance: entities.Report,
                                wire_df: pd.DataFrame, group_df: pd.DataFrame) -> entities.Report:
        report_df = await get_finrep_service(instance.category).create_report(wire_df, group_df, instance.interval)
        sheet = entities.SheetCreate(dataframe=report_df, drop_index=False, drop_columns=False, readonly_all_cells=True)
        await self.repo.overwrite_linked_sheet(instance, sheet)
        return instance
