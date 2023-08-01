import pandas as pd
import pandera as pa

from .wire import Wire


class Group:
    def __init__(self, df: pd.DataFrame = None):
        self.group = df.copy() if df is not None else None

    def create_group(self, wire: Wire, ccols: list[str]) -> None:
        raise NotImplemented

    def get_group_df(self) -> pd.DataFrame:
        if self.group is None:
            raise Exception(f"group is None; you probably miss create_group(wire: Wire, ccols: list[str]) function")
        return self.group


# todo need Nans validation
class ProfitGroupSchema(pa.DataFrameModel):
    reverse: pa.typing.Series[bool]


class ProfitGroup(Group):

    def __init__(self, df: pd.DataFrame = None):
        if df is not None:
            ProfitGroupSchema.validate(df)
        super().__init__(df)

    def create_group(self, wire: Wire, ccols: list[str]) -> None:
        df = wire.get_wire_df().copy()
        df = df[ccols].drop_duplicates().sort_values(ccols, ignore_index=True)

        levels = range(0, len(df.columns))
        for level in levels:
            name = f"level_{level + 1}"
            df[name] = df.iloc[:, level]
        df['reverse'] = False

        ProfitGroupSchema.validate(df)
        self.group = df


class BalanceGroup(Group):
    assets_key = 'assets'
    liabs_key = 'liabs'

    def create_group(self, wire: Wire, ccols: list[str]) -> None:
        df = wire.get_wire_df()
        df = df[ccols].drop_duplicates().sort_values(ccols, ignore_index=True)

        levels = range(0, len(df.columns))
        for level in levels:
            name = f"{self.assets_key}, level {level + 1}"
            df[name] = df.iloc[:, level]

        for level in levels:
            name = f"{self.liabs_key}, level {level + 1}"
            df[name] = df.iloc[:, level]

        self.group = df

    def get_gcols_assets(self, gcols: list[str]) -> list[str]:
        return [x for x in gcols if self.assets_key in x.lower()]

    def get_gcols_liabs(self, gcols: list[str]) -> list[str]:
        return [x for x in gcols if self.liabs_key in x.lower()]
