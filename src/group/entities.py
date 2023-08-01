import pandas as pd
from pydantic import BaseModel
import typing

from src import core_types
from . import enums


class InnerSource(BaseModel):
    id: core_types.Id_
    title: str
    updated_ad: pd.Timestamp


class Group(BaseModel):
    id: core_types.Id_
    title: str
    category: enums.GroupCategory
    columns: list[str]
    fixed_columns: list[str]
    source_id: core_types.Id_
    sheet_id: core_types.Id_
    updated_at: pd.Timestamp


class ExpandedGroup(Group):
    source: typing.Optional[InnerSource]
    sheet_df: typing.Optional[pd.DataFrame]

    class Config:
        arbitrary_types_allowed = True

    def to_json(self):
        return {
            "id": self.id,
            "title": self.title,
            "category": self.category,
            "columns": self.columns,
            "fixed_columns": self.fixed_columns,
            "source_id": self.source_id,
            "sheet_id": self.sheet_id,
            "updated_at": str(self.updated_at),
        }


Entity = typing.TypeVar(
    'Entity',
    bound=BaseModel,
)
