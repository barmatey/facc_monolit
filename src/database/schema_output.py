import typing

from .. import core_types
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
