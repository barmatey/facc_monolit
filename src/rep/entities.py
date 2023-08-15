import pydantic
from pydantic import BaseModel
from datetime import datetime
import typing

from src import core_types
from . import enums


class Interval(BaseModel):
    id: core_types.Id_
    period_year: int
    period_month: int
    period_day: int
    start_date: datetime
    end_date: datetime
    total_start_date: typing.Optional[datetime] = None
    total_end_date: typing.Optional[datetime] = None

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class InnerSource(BaseModel):
    id: core_types.Id_
    title: str
    updated_at: datetime

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class InnerGroup(BaseModel):
    id: core_types.Id_
    title: str
    ccols: list[str]
    fixed_ccols: list[str]
    updated_at: datetime
    sheet_id: core_types.Id_

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class InnerSheet(BaseModel):
    id: core_types.Id_
    updated_at: typing.Optional[datetime] = None

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class InnerCategory(BaseModel):
    id: core_types.Id_
    value: enums.ReportCategory


class Report(BaseModel):
    id: core_types.Id_
    title: str
    interval: Interval
    updated_at: datetime
    category: InnerCategory
    source: InnerSource
    group: InnerGroup
    sheet: InnerSheet

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
