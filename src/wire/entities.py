import typing
from typing import TypedDict, Literal

import pandas as pd
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
    total_start_date: pd.Timestamp
    total_end_date: pd.Timestamp
    wcols: list[Wcol]


class WireCreate(BaseModel):
    source_id: core_types.Id_
    date: pd.Timestamp
    sender: float
    receiver: float
    debit: float
    credit: float
    subconto_first: typing.Optional[str]
    subconto_second: typing.Optional[str]
    comment: typing.Optional[str]


class Wire(WireCreate):
    id: core_types.Id_


Entity = typing.Union[Source, Wire]
