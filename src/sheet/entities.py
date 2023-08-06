from typing import TypedDict

import pandas as pd
from pydantic import BaseModel

from src import core_types
from . import enums

"""
Sheet
"""


class SheetInfo(BaseModel):
    id: core_types.Id_
    updated_at: pd.Timestamp


class Cell(TypedDict):
    id: core_types.Id_
    value: str
    dtype: enums.Dtype
    is_readonly: bool
    is_filtred: bool
    is_index: bool
    text_align: enums.CellTextAlign
    color: str
    row_id: core_types.Id_
    col_id: core_types.Id_
    sheet_id: core_types.Id_


class Sindex(TypedDict):
    id: core_types.Id_
    index: int
    scroll_pos: int
    size: int
    is_freeze: bool
    sheet_id: core_types.Id_


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


"""
ColFilter & ColSorter
"""


class FilterItem(BaseModel):
    value: str
    dtype: enums.Dtype
    is_filtred: bool


class ColFilter(BaseModel):
    sheet_id: core_types.Id_
    col_id: core_types.Id_
    items: list[FilterItem]


class ColSorter(BaseModel):
    sheet_id: core_types.Id_
    col_id: core_types.Id_
    ascending: bool


class ColFilterRetrieve(BaseModel):
    sheet_id: core_types.Id_
    col_id: core_types.Id_
