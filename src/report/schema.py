import enum

import pydantic
import pandas as pd
import typing

from .. import core_types
from . import enums


class GroupCreateForm(pydantic.BaseModel):
    title: str
    category: enums.CategoryLiteral
    source_base: core_types.Id_
    sheet: str


class GroupRetrieveForm(pydantic.BaseModel):
    id_: core_types.Id_


class GroupDeleteForm(pydantic.BaseModel):
    id_: core_types.Id_


class ReportIntervalCreateForm(pydantic.BaseModel):
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    total_start_date: typing.Optional[pd.Timestamp]
    total_end_date: typing.Optional[pd.Timestamp]
    period_year: int
    period_month: int
    period_day: int


class ReportCreateForm(pydantic.BaseModel):
    title: str
    category: enums.CategoryLiteral
    interval: ReportIntervalCreateForm
    source_base: core_types.Id_
    group: core_types.Id_
    sheet: str
