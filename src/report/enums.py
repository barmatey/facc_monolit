import enum
import typing

CategoryLiteral = typing.Literal['BALANCE', 'PROFIT', 'CASHFLOW']


class Category(enum.Enum):
    BALANCE = 1
    PROFIT = 2
    CASHFLOW = 3
