import typing

import pandas as pd
import pydantic

from src import core_types
from . import entities

SourceSchema = entities.Source
SourceCreateSchema = entities.SourceCreate

WireSchema = entities.Wire
WireCreateSchema = entities.WireCreate


class WirePartialUpdateSchema(pydantic.BaseModel):
    date: typing.Optional[pd.Timestamp]
    sender: typing.Optional[float]
    receiver: typing.Optional[float]
    debit: typing.Optional[float]
    credit: typing.Optional[float]
    subconto_first: typing.Optional[str]
    subconto_second: typing.Optional[str]
    comment: typing.Optional[str]

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)


class SourceBulkRetrieveSchema(pydantic.BaseModel):
    filter_by: dict


class WireBulkRetrieveSchema(pydantic.BaseModel):
    filter_by: dict
    order_by: typing.Optional[core_types.OrderBy]
    ascending: typing.Optional[bool] = True
    paginate_from: typing.Optional[int]
    paginate_to: typing.Optional[int]
