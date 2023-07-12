import pandas as pd

from .wire import Wire


class BaseGroup:
    def __init__(self, wire: Wire, ccols: list[str]):
        self.wire = wire.get_wire_df().copy()
        self.ccols = ccols.copy()

        self.group = None

    async def create_group(self) -> None:
        df = self.wire

        df = df[self.ccols].drop_duplicates().sort_values(self.ccols, ignore_index=True)

        levels = range(0, len(df.columns))
        for level in levels:
            name = f"Level {level + 1}"
            df[name] = df.iloc[:, level]
        df['Reverse'] = False

        self.group = df

    async def get_group(self) -> pd.DataFrame:
        if self.group is None:
            raise Exception(f"group is None; you probably miss create_group() function")
        return self.group
