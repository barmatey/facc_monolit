from src import core_types
from src.core_types import DTO, OrderBy, Id_
from .repository import ReportRepository
from .entities import Report


class ReportService:

    def __init__(self, repo: ReportRepository):
        self.repo = repo

    async def create_one(self, data: DTO) -> Report:
        return await self.repo.create_one(data)

    async def get_one(self, filter_by: dict) -> Report:
        return await self.repo.get_one(filter_by)

    async def get_many(self, filter_by: dict, order_by: OrderBy = None) -> list[Report]:
        return await self.repo.get_many(filter_by, order_by)

    async def update_one(self, data: DTO, filter_by: dict) -> Report:
        return await self.repo.update_one(data, filter_by)

    async def delete_one(self, filter_by: dict) -> Id_:
        return await self.repo.delete_one(filter_by)

    async def add_linked_sheet(self, report: Report, sheet_id: core_types.Id_) -> Report:
        return await self.repo.add_linked_sheet(report, sheet_id)
