import typing

import pandas as pd

from src import core_types
from src.core_types import Event, Id_


class WireCreated(Event):
    source_id: Id_
    date: pd.Timestamp
    sender: float
    receiver: float
    debit: float
    credit: float
    subconto_first: typing.Optional[str]
    subconto_second: typing.Optional[str]
    comment: typing.Optional[str]


class WireManyCreated(Event):
    source_id: core_types.Id_
    wires: list[dict]


class SourceDatesInfoUpdated(Event):
    source_id: core_types.Id_


class WireDeleted(Event):
    wire_id: Id_


class SourceUpdated(Event):
    source_id: Id_
