import loguru
from pydantic import BaseModel
import pandas as pd

from src.core_types import Id_, OrderBy

from src.service_finrep import Finrep
from .entities import Entity, Group, ExpandedGroup
from .dto import CreateGroupRequest, CreateGroup, InnerCreateSheet
from .repository import CrudRepository, GroupRepository, WireRepository


class ServiceCrud:

    def __init__(self, repo: CrudRepository):
        self.repo = repo

    async def create_one(self, data: BaseModel) -> Entity:
        return await self.repo.create_one(data)

    async def get_one(self, filter_by: dict) -> Entity:
        return await self.repo.get_one(filter_by)

    async def get_many(self, filter_by: dict, order_by: OrderBy = None) -> list[Entity]:
        return await self.repo.get_many(filter_by, order_by)

    async def update_one(self, data: BaseModel, filter_by: dict) -> Entity:
        return await self.repo.update_one(data, filter_by)

    async def delete_one(self, filter_by: dict) -> Id_:
        return await self.repo.delete_one(filter_by)


class ServiceGroup(ServiceCrud):

    def __init__(self, group_repo: GroupRepository, wire_repo: WireRepository, finrep: Finrep):
        super().__init__(group_repo)
        self.group_repo = group_repo
        self.wire_repo = wire_repo
        self.finrep = finrep

    async def create_one(self, data: CreateGroupRequest) -> Group:
        wire_df = await self.wire_repo.get_wire_dataframe(filter_by={"source_id": data.source_id})
        group_df: pd.DataFrame = self.finrep.create_group(wire_df, data.columns)

        group_create = CreateGroup(
            title=data.title,
            source_id=data.source_id,
            columns=data.columns,
            fixed_columns=data.fixed_columns,
            dataframe=group_df,
            drop_index=True,
            drop_columns=False,
            category=data.category,
        )
        group: Group = await self.group_repo.create_one(group_create)
        return group

    async def get_one(self, filter_by: dict) -> ExpandedGroup:
        group: ExpandedGroup = await self.group_repo.get_expanded_one(filter_by)
        if group.updated_at <= group.source.updated_ad:
            raise TabError
        return group

    async def total_recalculate(self, instance: Group) -> ExpandedGroup:
        wire_df = await self.wire_repo.get_wire_dataframe({"source_id": instance.source_id})

        old_group_df = await self.group_repo.get_linked_dataframe(instance.id)
        new_group_df = await self.finrep.create_group(wire_df, instance.columns)

        if len(instance.fixed_columns):
            new_group_df = pd.merge(
                old_group_df[instance.fixed_columns],
                new_group_df,
                on=instance.fixed_columns, how='left'
            )

        data = InnerCreateSheet(
            df=new_group_df,
            drop_index=True,
            drop_columns=False,
            readonly_all_cells=False
        )
        await self.group_repo.overwrite_linked_sheet(instance, data)
        expanded_group = ExpandedGroup(**instance.dict(), sheet_df=new_group_df)
        return expanded_group
