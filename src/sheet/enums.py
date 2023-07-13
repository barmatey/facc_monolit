import enum
import typing

Dtype = typing.Literal['NUMBER', 'TEXT', 'BOOLEAN']


class CellDtype(enum.Enum):
    NUMBER = 'NUMBER'
    TEXT = 'TEXT'
    BOOLEAN = 'BOOLEAN'


CellTextAlign = typing.Literal['left', 'center', 'right']
