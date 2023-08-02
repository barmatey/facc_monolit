import typing

from .abstract import Finrep, BalanceFinrep, ProfitFinrep
from .wire import Wire
from .interval import Interval
from .group import Group, ProfitGroup, BalanceGroup
from .report import Report, ProfitReport, BalanceReport

Category = typing.Literal["BALANCE", "PROFIT", "CASHFLOW",]

FINREP = {
    "BALANCE": BalanceFinrep,
    "PROFIT": ProfitFinrep,
    "CASHFLOW": ProfitFinrep,
}


def get_finrep(category: Category | None = None) -> Finrep:
    if category is None:
        category = "BALANCE"
    return FINREP[category]()