import typing

import pandas as pd

from finrep_service import Wire


class Group:
    def __init__(self, df: pd.DataFrame = None):
        self.group = df.copy() if df is not None else None

    async def create_group(self, wire: Wire, ccols: list[str]) -> None:
        df = wire.get_wire_df().copy()
        df = df[ccols].drop_duplicates().sort_values(ccols, ignore_index=True)

        levels = range(0, len(df.columns))
        for level in levels:
            name = f"Level {level + 1}"
            df[name] = df.iloc[:, level]
        df['Reverse'] = False

        self.group = df

    def get_group_df(self) -> pd.DataFrame:
        if self.group is None:
            raise Exception(f"group is None; you probably miss create_group(wire: Wire, ccols: list[str]) function")
        return self.group


class ProfitGroup(Group):
    pass
