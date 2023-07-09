import typing

import pydantic

import core_types
from . import entities

SourceSchema = entities.Source
SourceCreateSchema = entities.SourceCreate

WireSchema = entities.Wire
WireCreateSchema = entities.WireCreate


class SourceBulkRetrieveSchema(pydantic.BaseModel):
    filter_by: dict


class WireBulkRetrieveSchema(pydantic.BaseModel):
    filter_by: dict
    order_by: typing.Optional[core_types.OrderBy]
    ascending: typing.Optional[bool] = True
    paginate_from: typing.Optional[int]
    paginate_to: typing.Optional[int]
