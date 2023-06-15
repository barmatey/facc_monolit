import enum

import numpy as np
import pydantic
import pandas as pd
import typing

from .. import core_types
from . import enums


class GroupCreateForm(pydantic.BaseModel):
    title: str
    category_id: enums.CategoryLiteral
    source_id: core_types.Id_
    sheet_id: str


class GroupRetrieveForm(pydantic.BaseModel):
    id_: core_types.Id_


class GroupDeleteForm(pydantic.BaseModel):
    id_: core_types.Id_


class ReportIntervalCreateForm(pydantic.BaseModel):
    period_year: int
    period_month: int
    period_day: int
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    total_start_date: typing.Optional[pd.Timestamp]
    total_end_date: typing.Optional[pd.Timestamp]


class ReportCreateForm(pydantic.BaseModel):
    title: str
    category: enums.CategoryLiteral
    interval: ReportIntervalCreateForm
    source_id: core_types.Id_
    group_id: core_types.Id_
    sheet_id: str
