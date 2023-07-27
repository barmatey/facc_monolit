from enum import Enum

import core_types
from core_types import OrderBy
from report.entities import ReportCategory
from report.repository import Entity
from repository_postgres.category import Category as CategoryModel
from src.report.repository import CrudRepo

from .base import BasePostgres


class CategoryEnum(Enum):
    BALANCE = 1
    PROFIT = 2
    CASHFLOW = 3


class CategoryRepoPostgres(BasePostgres, CrudRepo):
    model = CategoryModel

    async def create_one(self, data: core_types.DTO) -> ReportCategory:
        model = await super().create_one(data)
        return model.to_entity()

    async def get_one(self, filter_by: dict) -> ReportCategory:
        model = await super().get_one(filter_by)
        return model.to_entity()

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True, slice_from: int = None,
                       slice_to: int = None) -> list[ReportCategory]:
        models = await super().get_many(filter_by, order_by, asc, slice_from, slice_to)
        return [x.to_entity() for x in models]

    async def update_one(self, data: core_types.DTO, filter_by: dict) -> ReportCategory:
        raise NotImplemented

    async def delete_one(self, filter_by: dict) -> core_types.Id_:
        raise NotImplemented
