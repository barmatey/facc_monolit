import typing
from typing import TypedDict

import pandas as pd
from pydantic import BaseModel

from src import core_types
from . import enums


class SindexCreate(TypedDict):
    index: int
    scroll_pos: int
    size: int
    is_freeze: bool
    sheet_id: bool


class Sindex(SindexCreate):
    id: core_types.Id_


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
    readonly_all_cells: bool = False

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


class ColFilterRetrieve(TypedDict):
    sheet_id: core_types.Id_
    col_id: core_types.Id_


class FilterItem(TypedDict):
    value: str
    dtype: enums.Dtype
    is_filtred: bool


class ColFilter(ColFilterRetrieve):
    items: list[FilterItem]


class ColSorter(BaseModel):
    sheet_id: core_types.Id_
    col_id: core_types.Id_
    ascending: bool


class CopySindex(BaseModel):
    id: core_types.Id_
    index: int
    sheet_id: int


class CopyCell(BaseModel):
    value: str
    dtype: enums.Dtype
    row_index: int
    col_index: int
    sheet_id: core_types.Id_


class UpdateSindexSize(BaseModel):
    sindex_id: core_types.Id_
    new_size: int


class UpdateCell(BaseModel):
    id: core_types.Id_
    value: str
    dtype: enums.Dtype
    is_selected: typing.Optional[bool]
    is_readonly: typing.Optional[bool]
    is_filtred: typing.Optional[bool]
