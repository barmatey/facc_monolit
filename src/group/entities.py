import pandas as pd
from pydantic import BaseModel
import typing

from src import core_types
from . import enums


class InnerCategory(BaseModel):
    id: core_types.Id_
    value: enums.GroupCategory


class InnerSource(BaseModel):
    id: core_types.Id_
    title: str
    updated_at: pd.Timestamp


class InnerSheet(BaseModel):
    id: core_types.Id_
    updated_at: pd.Timestamp


class Group(BaseModel):
    id: core_types.Id_
    title: str
    category: InnerCategory
    ccols: list[str]
    fixed_columns: list[str]
    source: InnerSource
    sheet: InnerSheet
    updated_at: pd.Timestamp
    sheet_df: typing.Optional[pd.DataFrame]

    class Config:
        arbitrary_types_allowed = True

    def to_json(self):
        return {
            "id": self.id,
            "title": self.title,
            "category": self.category.value,
            "columns": self.ccols,
            "fixed_columns": self.fixed_columns,
            "source_id": self.source.id,
            "sheet_id": self.sheet.id,
            "updated_at": str(self.updated_at),
        }


Entity = typing.TypeVar(
    'Entity',
    bound=BaseModel,
)
