import typing

import pandas as pd
import pydantic

from .. import core_types
from . import enums
from . import entities

ReportCategorySchema = enums.CategoryLiteral


class GroupCreateSchema(pydantic.BaseModel):
    title: str
    category: enums.CategoryLiteral
    source_id: core_types.Id_
    columns: list[str]


class GroupRetrieveSchema(pydantic.BaseModel):
    id: core_types.Id_
    title: str
    category: enums.CategoryLiteral
    source_id: core_types.Id_
    sheet_id: core_types.Id_

    @classmethod
    def from_group_entity(cls, data: entities.Group) -> typing.Self:
        params = {
            'id': data.id,
            'title': data.title,
            'category': data.category.name,
            'source_id': data.source_id,
            'sheet_id': data.sheet_id,
        }
        return cls(**params)


class ReportIntervalCreateSchema(pydantic.BaseModel):
    start_date: pd.Timestamp
    end_date: pd.Timestamp
    iyear: int
    imonth: int
    iday: int


class ReportCreateSchema(pydantic.BaseModel):
    title: str
    category: enums.CategoryLiteral
    interval: ReportIntervalCreateSchema
    group_id: core_types.Id_
    source_id: core_types.Id_


ReportSchema = entities.Report
