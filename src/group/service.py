from pydantic import BaseModel
from src.core_types import Id_, OrderBy, DTO

from .entities import Entity, ExpandedGroup
from .repository import CrudRepository, GroupRepository


class ServiceCrud:

    def __init__(self, repo: CrudRepository):
        self.repo = repo

    async def create_one(self, data: BaseModel) -> Entity:
        return await self.repo.create_one(data)

    async def get_one(self, filter_by: dict) -> Entity:
        return await self.repo.get_one(filter_by)

    async def get_many(self, filter_by: dict, order_by: OrderBy = None) -> list[Entity]:
        return await self.repo.get_many(filter_by, order_by)

    async def update_one(self, data: DTO, filter_by: dict) -> Entity:
        return await self.repo.update_one(data, filter_by)

    async def delete_one(self, filter_by: dict) -> Id_:
        return await self.repo.delete_one(filter_by)


class GroupService(ServiceCrud):

    def __init__(self, group_repo: GroupRepository):
        super().__init__(group_repo)
        self.group_repo = group_repo

    async def get_one(self, filter_by: dict) -> ExpandedGroup:
        group: ExpandedGroup = await self.group_repo.get_expanded_one(filter_by)
        return group
