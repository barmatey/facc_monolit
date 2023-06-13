import pydantic
import pandas as pd
import typing

from .. import core_types


class GroupCreate(pydantic.BaseModel):
    wire_base_id: core_types.Id_
    columns: list[str]


class ReportInterval(pydantic.BaseModel):
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    total_start_date: typing.Optional[pd.Timestamp]
    total_end_date: typing.Optional[pd.Timestamp]
    iyear: int
    imonth: int
    iday: int


class ReportCreate(pydantic.BaseModel):
    wire_base_id: core_types.Id_
    group_id: core_types.Id_
    interval: ReportInterval
