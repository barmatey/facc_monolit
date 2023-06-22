import pandas as pd
from pydantic import BaseModel

from src import core_types


class SourceCreateData(BaseModel):
    title: str


class Source(BaseModel):
    id: core_types.Id_
    title: str
    total_start_date: pd.Timestamp
    total_end_date: pd.Timestamp
    wcols: list[str]


class Wire(BaseModel):
    pass
