import typing
from typing import TypedDict

import pandas as pd
from pydantic import BaseModel

from src import core_types


class SourceCreate(BaseModel):
    title: str


class Source(BaseModel):
    id: core_types.Id_
    title: str
    total_start_date: pd.Timestamp
    total_end_date: pd.Timestamp
    wcols: list[str]


class WireCreate(TypedDict):
    source_id: core_types.Id_
    date: pd.Timestamp
    sender: float
    receiver: float
    debit: float
    credit: float
    subconto_first: str
    subconto_second: str
    comment: str


class Wire(WireCreate):
    id: core_types.Id_


Entity = typing.Union[Source, Wire]
