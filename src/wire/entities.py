import typing
from typing import Literal
from typing_extensions import TypedDict

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
    total_start_date: pd.Timestamp
    total_end_date: pd.Timestamp
    wcols: list[Wcol]
    updated_at: pd.Timestamp

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


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

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class Wire(WireCreate):
    id: core_types.Id_


Entity = typing.Union[Source, Wire]
