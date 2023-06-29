import enum
import typing

CategoryLiteral = typing.Literal['BALANCE', 'PROFIT', 'CASHFLOW']


# todo move this enum in repository_postgres

class Category(enum.Enum):
    BALANCE = 1
    PROFIT = 2
    CASHFLOW = 3
