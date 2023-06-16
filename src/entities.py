import typing

import pandas as pd
from pydantic import BaseModel

from . import core_types

class Category(BaseModel):
    pass


class GroupCreateData(BaseModel):
    title: str
    category_id: core_types.Id_
    source_id: core_types.Id_
    sheet_id: str


class Group(GroupCreateData):
    id: core_types.Id_


class Interval(BaseModel):
    period_year: int
    period_month: int
    period_day: int
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    total_start_date: typing.Optional[pd.Timestamp]
    total_end_date: typing.Optional[pd.Timestamp]


class SheetCreateData(BaseModel):
    pass


class Sheet(SheetCreateData):
    pass


class Report(BaseModel):
    title: str
    category_id: core_types.Id_
    source_id: core_types.Id_
    group_id: core_types.Id_
    sheet_id: core_types.Id_
