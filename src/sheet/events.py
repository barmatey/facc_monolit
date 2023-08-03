import typing

import pandas as pd

from src import core_types
from src.core_types import Event


class SheetCreated(Event):
    df: pd.DataFrame
    drop_index: bool
    drop_columns: bool
    readonly_all_cells: bool = False

    class Config:
        arbitrary_types_allowed = True


class SheetGotten(Event):
    sheet_id: core_types.Id_
    from_scroll: typing.Optional[int]
    to_scroll: typing.Optional[int]
