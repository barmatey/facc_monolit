import typing

import pandas as pd
from pydantic import BaseModel

from . import enums
from .. import core_types


class GroupCreate(BaseModel):
    title: str
    category: enums.Category
    source_id: int
    columns: list[str]
    dataframe: pd.DataFrame
    drop_index: bool
    drop_columns: bool

    class Config:
        arbitrary_types_allowed = True


class GroupRetrieve(BaseModel):
    id: core_types.Id_
    title: str
    category: enums.Category
    source_id: core_types.Id_
    sheet_id: core_types.Id_


class ReportIntervalCreate(BaseModel):
    period_year: int
    period_month: int
    period_day: int
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    total_start_date: typing.Optional[pd.Timestamp]
    total_end_date: typing.Optional[pd.Timestamp]


class ReportInterval(ReportIntervalCreate):
    id: core_types.Id_


class ReportCreate(BaseModel):
    title: str
    category_id: core_types.Id_
    source_id: core_types.Id_
    group_id: core_types.Id_


class Report(ReportCreate):
    id: core_types.Id_
    sheet_id: core_types.MongoId
