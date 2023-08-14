import pydantic
from pydantic import BaseModel
import pandas as pd
import typing

from src import core_types
from . import enums


class Interval(BaseModel):
    id: core_types.Id_
    period_year: int
    period_month: int
    period_day: int
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    total_start_date: typing.Optional[pd.Timestamp]
    total_end_date: typing.Optional[pd.Timestamp]

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class InnerSource(BaseModel):
    id: core_types.Id_
    title: str
    updated_at: pd.Timestamp

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class InnerGroup(BaseModel):
    id: core_types.Id_
    title: str
    ccols: list[str]
    fixed_ccols: list[str]
    updated_at: pd.Timestamp
    sheet_id: core_types.Id_

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class InnerSheet(BaseModel):
    id: core_types.Id_
    updated_at: typing.Optional[pd.Timestamp]

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class InnerCategory(BaseModel):
    id: core_types.Id_
    value: enums.ReportCategory


class Report(BaseModel):
    id: core_types.Id_
    title: str
    interval: Interval
    updated_at: pd.Timestamp
    category: InnerCategory
    source: InnerSource
    group: InnerGroup
    sheet: InnerSheet

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
