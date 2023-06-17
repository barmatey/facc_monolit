import typing

import pandas as pd
from pydantic import BaseModel

from src import core_types

Dtype = typing.Literal['NUMBER', 'TEXT', 'BOOLEAN']


class SheetCreate(BaseModel):
    pass


class SindexCreate(BaseModel):
    size: int
    is_freeze: bool
    is_filtred: bool
    sheet_id: core_types.Id_


class Sindex(SindexCreate):
    id: core_types.Id_


class CellCreate(BaseModel):
    value: str
    dtype: Dtype
    is_index: bool
    is_readonly: bool
    is_filtred: bool
    sheet_id: core_types.Id_


class Cell(CellCreate):
    id: core_types.Id_


class Sheet(BaseModel):
    id: core_types.Id_
    rows: list[Sindex]
    cols: list[Sindex]
    cells: list[Cell]
