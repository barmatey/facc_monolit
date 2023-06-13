import typing

import pydantic

from ... import core_types
from . import schema_input


class Sindex(schema_input.Sindex):
    id_: core_types.Id_


class TableCell(schema_input.TableCell):
    id_: core_types.Id_


class Sheet(schema_input.Sheet):
    id_: core_types.Id_


class FilterItem(typing.TypedDict):
    value: schema_input.CellValue
    is_filtred: bool


class ScrollSize(pydantic.BaseModel):
    scroll_height: int
    scroll_width: int
    count_rows: int
    count_cols: int
