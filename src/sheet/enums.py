import enum
import typing

Dtype = typing.Literal['NUMBER', 'TEXT', 'BOOLEAN']


class CellDtype(enum.Enum):
    NUMBER = 'NUMBER'
    BOOLEAN = 'BOOLEAN'
    TEXT = 'TEXT'
