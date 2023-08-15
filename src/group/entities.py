import pandas as pd
import pydantic
from pydantic import BaseModel
import typing
from datetime import datetime

from src import core_types
from . import enums


class InnerCategory(BaseModel):
    id: core_types.Id_
    value: enums.GroupCategory


class InnerSource(BaseModel):
    id: core_types.Id_
    title: str
    updated_at: datetime


class InnerSheet(BaseModel):
    id: core_types.Id_
    updated_at: datetime


class Group(BaseModel):
    id: core_types.Id_
    title: str
    category: InnerCategory
    ccols: list[str]
    fixed_ccols: list[str]
    source: InnerSource
    sheet: InnerSheet
    updated_at: datetime
    sheet_df: typing.Optional[pd.DataFrame] = None

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    def to_json(self):
        return {
            "id": self.id,
            "title": self.title,
            "category": self.category.value,
            "ccols": self.ccols,
            "fixed_ccols": self.fixed_ccols,
            "source_id": self.source.id,
            "sheet_id": self.sheet.id,
            "updated_at": str(self.updated_at),
        }


Entity = typing.TypeVar(
    'Entity',
    bound=BaseModel,
)
