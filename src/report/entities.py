import typing

import pandas as pd
from datetime import datetime
import pydantic
from pydantic import BaseModel

from src import core_types

from . import enums


class SheetCreate(BaseModel):
    dataframe: pd.DataFrame
    drop_index: bool
    drop_columns: bool
    readonly_all_cells: bool = False

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class GroupCreate(BaseModel):
    title: str
    category: enums.CategoryLiteral
    source_id: int
    columns: list[str]
    fixed_columns: list[str]
    dataframe: pd.DataFrame
    drop_index: bool
    drop_columns: bool

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class Group(BaseModel):
    id: core_types.Id_
    title: str
    category: enums.CategoryLiteral
    columns: list[str]
    fixed_columns: list[str]
    source_id: core_types.Id_
    sheet_id: core_types.Id_


class ExpandedGroup(Group):
    sheet_df: pd.DataFrame

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class IntervalCreate(BaseModel):
    period_year: int
    period_month: int
    period_day: int
    start_date: datetime
    end_date: datetime
    total_start_date: typing.Optional[datetime] = None
    total_end_date: typing.Optional[datetime] = None


class Interval(IntervalCreate):
    id: core_types.Id_


class ReportCreate(BaseModel):
    title: str
    category: enums.CategoryLiteral
    source_id: core_types.Id_
    group_id: core_types.Id_
    interval: IntervalCreate
    sheet: SheetCreate


class Report(BaseModel):
    id: core_types.Id_
    title: str
    category: enums.CategoryLiteral
    source_id: core_types.Id_
    group_id: core_types.Id_
    interval: Interval
    sheet_id: core_types.Id_

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class ReportCategoryCreate(BaseModel):
    value: enums.CategoryLiteral


class ReportCategory(BaseModel):
    id: core_types.Id_
    value: enums.CategoryLiteral


Entity = typing.Union[Group, Report, ReportCategory]
