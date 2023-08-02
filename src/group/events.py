from pydantic import BaseModel
import pandas as pd

from src import core_types
from . import enums
from .entities import Group


class InnerCreateSheet(core_types.Event):
    df: pd.DataFrame
    drop_index: bool
    drop_columns: bool
    readonly_all_cells: bool = False

    class Config:
        arbitrary_types_allowed = True


class CreateGroupRequest(BaseModel):
    title: str
    category: enums.GroupCategory
    source_id: core_types.Id_
    columns: list[str]
    fixed_columns: list[str]


class CreateGroup(BaseModel):
    title: str
    category: enums.GroupCategory
    source_id: int
    columns: list[str]
    fixed_columns: list[str]
    dataframe: pd.DataFrame
    drop_index: bool
    drop_columns: bool

    class Config:
        arbitrary_types_allowed = True


class GroupGotten(core_types.Event):
    group_id: core_types.Id_


class SourceUpdated(core_types.Event):
    group_instance: Group
