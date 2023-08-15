import typing

import pandas as pd
from datetime import datetime

from src import core_types
from src.core_types import Event, Id_


class SourceCreated(Event):
    title: str


class WireCreated(Event):
    source_id: Id_
    date: datetime
    sender: float
    receiver: float
    debit: float
    credit: float
    subconto_first: typing.Optional[str] = None
    subconto_second: typing.Optional[str] = None
    comment: typing.Optional[str] = None


class WireManyCreated(Event):
    source_id: core_types.Id_
    wires: list[dict]


class WirePartialUpdated(Event):
    wire_id: typing.Optional[core_types.Id_] = None
    date: typing.Optional[datetime] = None
    sender: typing.Optional[float] = None
    receiver: typing.Optional[float] = None
    debit: typing.Optional[float] = None
    credit: typing.Optional[float] = None
    subconto_first: typing.Optional[str] = None
    subconto_second: typing.Optional[str] = None
    comment: typing.Optional[str] = None


class SourceDatesInfoUpdated(Event):
    source_id: core_types.Id_


class WireDeleted(Event):
    wire_id: Id_


class SourceUpdated(Event):
    source_id: Id_
