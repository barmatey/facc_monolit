import typing
from typing import Literal
from typing_extensions import TypedDict
from datetime import datetime

import pandas as pd
import pydantic
from pydantic import BaseModel

from src import core_types


class SourceCreate(BaseModel):
    title: str


class Wcol(TypedDict):
    title: str
    label: str
    dtype: Literal['str', 'float', 'date']


class Source(BaseModel):
    id: core_types.Id_
    title: str
    total_start_date: datetime
    total_end_date: datetime
    wcols: list[Wcol]
    updated_at: datetime

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class WireCreate(BaseModel):
    source_id: core_types.Id_
    date: datetime
    sender: float
    receiver: float
    debit: float
    credit: float
    subconto_first: typing.Optional[str] = None
    subconto_second: typing.Optional[str] = None
    comment: typing.Optional[str] = None

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class Wire(WireCreate):
    id: core_types.Id_


Entity = typing.Union[Source, Wire]

"""
Source Plan
"""


class PlanItem(BaseModel):
    sender: float
    receiver: float
    sub1: str
    sub2: str
    source_id: core_types.Id_
