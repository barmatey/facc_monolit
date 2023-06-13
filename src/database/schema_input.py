import enum
import typing
from typing import TypedDict
import pydantic

from .. import core_types

CellValue = str


class CellDtype(enum.Enum):
    TEXT = 'text'


class Sindex(TypedDict):
    size: int
    is_freeze: bool
    is_filtred: bool


class TableCell(TypedDict):
    value: CellValue
    dtype: CellDtype
    is_index: bool
    is_readonly: bool
    is_filtred: bool


class Sheet(TypedDict):
    rows: list[Sindex]
    cols: list[Sindex]
    cells: list[TableCell]


class RetrieveSheetForm(pydantic.BaseModel):
    sheet_id: core_types.Id_
    from_scroll_position: typing.Optional[int]
    to_scroll_position: typing.Optional[int]


class SortSheetForm(RetrieveSheetForm):
    sort_by_col: core_types.Id_
    ascending: bool


class UpdateFilterItemForm(pydantic.BaseModel):
    value: str
    is_filtred: bool


class UpdateColFilterForm(pydantic.BaseModel):
    col_id: core_types.Id_
    items: list[UpdateFilterItemForm]
