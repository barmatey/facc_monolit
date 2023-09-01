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


class DeleteManyRecordsSchema(BaseModel):
    record_ids: list[core_types.Id_]
