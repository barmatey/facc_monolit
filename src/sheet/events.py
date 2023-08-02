import pandas as pd

from src.core_types import Event


class SheetCreated(Event):
    df: pd.DataFrame
    drop_index: bool
    drop_columns: bool
    readonly_all_cells: bool = False

    class Config:
        arbitrary_types_allowed = True
