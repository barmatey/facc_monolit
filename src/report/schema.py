import typing

import pandas as pd
import pydantic

from .. import core_types
from . import enums
from . import entities

ReportCategorySchema = enums.CategoryLiteral


class GroupCreateSchema(pydantic.BaseModel):
    title: str
    category: enums.CategoryLiteral
    source_id: core_types.Id_
    columns: list[str]


GroupSchema = entities.Group


class ReportIntervalCreateSchema(pydantic.BaseModel):
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    iyear: int
    imonth: int
    iday: int


class ReportCreateSchema(pydantic.BaseModel):
    title: str
    category: enums.CategoryLiteral
    interval: ReportIntervalCreateSchema
    group_id: core_types.Id_
    source_id: core_types.Id_


ReportSchema = entities.Report
