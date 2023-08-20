import typing

import pandas as pd
import pydantic
from pydantic import BaseModel

from src import core_types
from . import entities

SourceSchema = entities.Source
SourceCreateSchema = entities.SourceCreate

WireSchema = entities.Wire
WireCreateSchema = entities.WireCreate


class PlanItemPartialUpdateSchema(BaseModel):
    id: core_types.Id_
    sender: typing.Optional[float] = None
    receiver: typing.Optional[float] = None
    sub1: typing.Optional[str] = None
    sub2: typing.Optional[str] = None


class DeleteManyRecordsSchema(BaseModel):
    record_ids: list[core_types.Id_]
