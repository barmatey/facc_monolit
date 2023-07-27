from sqlalchemy.ext.asyncio import AsyncSession

from core_types import OrderBy
from src import core_types
from src.report.repository import Entity
from src.report.repository import WireRepo

from .base import BasePostgres


class WireRepoPostgres(BasePostgres, WireRepo):
    def __init__(self, session: AsyncSession):
        super().__init__(session)

    async def create_one(self, data: core_types.DTO) -> Entity:
        raise NotImplemented

    async def get_one(self, filter_by: dict) -> Entity:
        raise NotImplemented

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                       slice_from: int = None, slice_to: int = None):
        raise NotImplemented

    async def update_one(self, data: core_types.DTO, filter_by: dict) -> Entity:
        raise NotImplemented

    async def delete_one(self, filter_by: dict) -> core_types.Id_:
        raise NotImplemented
