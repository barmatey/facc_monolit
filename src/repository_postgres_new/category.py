from enum import Enum

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from src import core_types
from src.core_types import OrderBy
from src.report import enums
from src.report.entities import ReportCategory
from src.report.repository import CrudRepo

from .base import BasePostgres, BaseModel


class CategoryModel(BaseModel):
    __tablename__ = "category"
    value: Mapped[str] = mapped_column(String(30), nullable=False, unique=True)

    def to_category_literal(self) -> enums.CategoryLiteral:
        return self.value

    def to_entity(self) -> ReportCategory:
        return ReportCategory(
            id=self.id,
            value=self.value,
        )


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
        deleted_model = await super().delete_one(filter_by)
        return deleted_model.id
