import typing

import loguru
import pandas as pd
from pydantic import BaseModel

from src import core_types
from .service_finrep import get_finrep_service
from . import repository
from . import entities, schema

OrderBy = typing.Union[str, list[str]]
DTO = typing.Union[BaseModel]


class Service:
    repo: repository.CrudRepo

    async def create(self, data: BaseModel) -> entities.Entity:
        return await self.repo.create(data)

    async def retrieve(self, filter_by: dict) -> entities.Entity:
        return await self.repo.retrieve(filter_by)

    async def retrieve_bulk(self, filter_by: dict, order_by: OrderBy = None) -> list[entities.Entity]:
        return await self.repo.retrieve_bulk(filter_by, order_by)

    async def partial_update(self, data: BaseModel, filter_by: dict) -> entities.Entity:
        return await self.repo.update(data, filter_by)

    async def delete(self, filter_by: dict) -> core_types.Id_:
        return await self.repo.delete(filter_by)


class CategoryService(Service):
    repo: repository.CrudRepo = repository.CategoryRepo()


class GroupService(Service):
    repo = repository.GroupRepo()
    wire_repo = repository.WireRepo()

    async def create(self, data: schema.GroupCreateSchema) -> entities.Group:
        wire_df = await self.wire_repo.retrieve_wire_dataframe(filter_by={"source_id": data.source_id})
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
        group: entities.Group = await self.repo.create(group_create)
        return group

    async def total_recalculate(self, instance: entities.Group) -> entities.Group:
        wire_df = await self.wire_repo.retrieve_wire_dataframe(filter_by={"source_id": instance.source_id})

        old_group_df = await self.repo.retrieve_linked_sheet_as_dataframe(instance.id)
        new_group_df = await get_finrep_service(instance.category).create_group(wire_df, instance.columns)
        new_group_df = pd.merge(
            old_group_df[instance.fixed_columns],
            new_group_df,
            on=instance.fixed_columns, how='left'
        )
        data = schema.GroupSheetUpdateSchema(
            df=new_group_df,
            drop_index=True,
            drop_columns=False,
        )
        updated: entities.Group = await self.repo.update_sheet(instance, data)
        return updated


class ReportService(Service):
    repo: repository.CrudRepo = repository.ReportRepo()
    wire_repo = repository.WireRepo()
    group_repo = repository.GroupRepo()

    async def create(self, data: schema.ReportCreateSchema) -> entities.Report:
        wire_df = await self.wire_repo.retrieve_wire_dataframe(filter_by={"source_id": data.source_id})
        group_df = await self.group_repo.retrieve_linked_sheet_as_dataframe(group_id=data.group_id)

        report_df = await get_finrep_service(data.category).create_report(wire_df, group_df, data.interval)
        sheet = entities.SheetCreate(dataframe=report_df, drop_index=False, drop_columns=False, readonly_all_cells=True)
        report_create = entities.ReportCreate(
            title=data.title,
            category=data.category,
            source_id=data.source_id,
            group_id=data.group_id,
            interval=data.interval.to_interval_create_entity(),
            sheet=sheet,
        )
        report = await self.repo.create(report_create)
        return report
