from enum import Enum

from repository_postgres.category import Category as CategoryModel
from src.report.repository import CrudRepo

from .base import BasePostgres


class CategoryEnum(Enum):
    BALANCE = 1
    PROFIT = 2
    CASHFLOW = 3


class CategoryRepoPostgres(BasePostgres, CrudRepo):
    model = CategoryModel
