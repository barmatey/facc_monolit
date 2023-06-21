import typing
from typing import TypedDict

import pandas as pd
from pydantic import BaseModel

import core_types
from . import enums


class SindexCreate(TypedDict):
    index: int
    scroll_pos: int
    size: int
    is_freeze: bool
    sheet_id: bool


class Sindex(TypedDict):
    id: core_types.Id_
    sheet_id: core_types.Id_


class CellCreate(TypedDict):
    value: str
    dtype: enums.Dtype
    is_readonly: bool
    is_filtred: bool
    is_index: bool
    color: str


class Cell(TypedDict):
    id: core_types.Id_
    row_id: core_types.Id_
    col_id: core_types.Id_
    sheet_id: core_types.Id_


class SheetCreate(BaseModel):
    df: pd.DataFrame
    drop_index: bool
    drop_columns: bool

    class Config:
        arbitrary_types_allowed = True


class SheetRetrieve(BaseModel):
    sheet_id: core_types.Id_
    from_scroll: typing.Optional[int]
    to_scroll: typing.Optional[int]


class Sheet(TypedDict):
    id: core_types.Id_
    rows: list[Sindex]
    cols: list[Sindex]
    cells: list[Cell]


class ScrollSize(BaseModel):
    count_rows: int
    count_cols: int
    scroll_height: int
    scroll_width: int


class FilterItem(TypedDict):
    value: str
    dtype: enums.Dtype
    is_filtred: bool


class ColFilter(TypedDict):
    col_id: core_types.Id_
    items: list[FilterItem]

