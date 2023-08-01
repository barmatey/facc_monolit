import pandas as pd
from pydantic import BaseModel
import typing

from src import core_types
from . import enums


class InnerSource(BaseModel):
    id: core_types.Id_
    title: str


class Group(BaseModel):
    id: core_types.Id_
    title: str
    category: enums.GroupCategory
    columns: list[str]
    fixed_columns: list[str]
    source_id: core_types.Id_
    sheet_id: core_types.Id_


class ExpandedGroup(Group):
    source: typing.Optional[InnerSource]
    sheet_df: typing.Optional[pd.DataFrame]

    class Config:
        arbitrary_types_allowed = True


Entity = typing.TypeVar(
    'Entity',
    bound=BaseModel,
)
