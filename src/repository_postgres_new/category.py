from enum import Enum

import loguru
from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from report import entities
from src.report import enums
from .base import BaseModel, BaseRepo


class CategoryEnum(Enum):
    BALANCE = 1
    PROFIT = 2
    CASHFLOW = 3


class Category(BaseModel):
    __tablename__ = "category"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    value: Mapped[str] = mapped_column(String(30), nullable=False, unique=True)

    def to_category_literal(self) -> enums.CategoryLiteral:
        return self.value

    def to_entity(self) -> entities.ReportCategory:
        return entities.ReportCategory(
            id=self.id,
            value=self.value,
        )


class CategoryRepo(BaseRepo):
    model = Category

    # async def retrieve_bulk(self, filter_: dict = None,
    #                         sort_by: str = None, ascending=True) -> list[enums.CategoryLiteral]:
    #     # noinspection PyTypeChecker
    #     categories: list[Category] = await self.re(filter_={})
    #     return [c.to_category_literal() for c in categories]
