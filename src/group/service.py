import pandas as pd
from pydantic import BaseModel
from src.core_types import Id_, OrderBy, DTO

from .entities import Entity, Group
from .repository import GroupRepository


class GroupService:

    def __init__(self, repo: GroupRepository):
        self.repo = repo

    async def create_one(self, data: BaseModel) -> Group:
        return await self.repo.create_one(data)

    async def get_one(self, filter_by: dict) -> Group:
        group: Group = await self.repo.get_one(filter_by)
        return group

    async def get_linked_frame(self, group_id: Id_) -> pd.DataFrame:
        df = await self.repo.get_linked_dataframe(group_id)
        return df

    async def get_many(self, filter_by: dict, order_by: OrderBy = None) -> list[Group]:
        return await self.repo.get_many(filter_by, order_by)

    async def update_one(self, data: DTO, filter_by: dict) -> Group:
        return await self.repo.update_one(data, filter_by)

    async def delete_one(self, filter_by: dict) -> Id_:
        return await self.repo.delete_one(filter_by)
