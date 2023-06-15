import enum

import pydantic
import pandas as pd
import typing

from .. import core_types


class GroupCreateForm(pydantic.BaseModel):
    title: str
    category: core_types.Id_
    source_base: core_types.Id_
    sheet: str


class GroupRetrieveForm(pydantic.BaseModel):
    id_: core_types.Id_


class GroupDeleteForm(pydantic.BaseModel):
    id_: core_types.Id_


class ReportInterval(pydantic.BaseModel):
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    total_start_date: typing.Optional[pd.Timestamp]
    total_end_date: typing.Optional[pd.Timestamp]
    iyear: int
    imonth: int
    iday: int


class ReportCreateForm(pydantic.BaseModel):
    wire_base_id: core_types.Id_
    group_id: core_types.Id_
    interval: ReportInterval
