import typing

import pandas as pd
import pydantic

from .. import core_types
from . import enums
from . import entities

ReportCategorySchema = enums.CategoryLiteral
ReportCategoryCreateSchema = entities.ReportCategoryCreate


class GroupCreateSchema(pydantic.BaseModel):
    title: str
    category: enums.CategoryLiteral
    source_id: core_types.Id_
    columns: list[str]
    fixed_columns: list[str]


class GroupSheetUpdateSchema(pydantic.BaseModel):
    df: pd.DataFrame
    drop_index: bool
    drop_columns: bool

    class Config:
        arbitrary_types_allowed = True


GroupSchema = entities.Group


class IntervalCreateSchema(pydantic.BaseModel):
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    iyear: int
    imonth: int
    iday: int

    def to_interval_create_entity(self) -> entities.IntervalCreate:
        return entities.IntervalCreate(
            start_date=self.start_date,
            end_date=self.end_date,
            period_year=self.iyear,
            period_month=self.imonth,
            period_day=self.iday,
        )


class ReportCreateSchema(pydantic.BaseModel):
    title: str
    category: enums.CategoryLiteral
    interval: IntervalCreateSchema
    group_id: core_types.Id_
    source_id: core_types.Id_


ReportSchema = entities.Report
