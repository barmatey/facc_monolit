import enum
import typing

Dtype = typing.Literal['NUMBER', 'TEXT', 'BOOLEAN', 'DATE', ]


class CellDtype(enum.Enum):
    NUMBER = 'NUMBER'
    TEXT = 'TEXT'
    BOOLEAN = 'BOOLEAN'
    DATE = 'DATE'


CellTextAlign = typing.Literal['left', 'center', 'right']
