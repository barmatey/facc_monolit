import typing

import pandas as pd
from pydantic import BaseModel

from .. import core_types
from . import enums


class SindexCreate(typing.TypedDict):
    size: int
    is_freeze: bool
    is_filtred: bool
    sheet_id: core_types.Id_


class Sindex(SindexCreate):
    id: core_types.Id_


class CellCreate(typing.TypedDict):
    value: str
    dtype: enums.Dtype
    is_index: bool
    is_readonly: bool
    is_filtred: bool
    sheet_id: core_types.Id_


class Cell(CellCreate):
    id: core_types.Id_


class SheetCreate(BaseModel):
    df: pd.DataFrame
    drop_index: bool
    drop_columns: bool


class Sheet(BaseModel):
    id: core_types.Id_
    rows: list[Sindex]
    cols: list[Sindex]
    cells: list[Cell]
