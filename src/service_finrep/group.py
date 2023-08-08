import loguru
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

    @staticmethod
    def merge_groups(old_group_df: pd.DataFrame,
                     new_group_df: pd.DataFrame,
                     ccols: list[str],
                     fixed_ccols: list[str]
                     ) -> pd.DataFrame:
        if len(fixed_ccols):
            new_group_df = pd.merge(
                old_group_df[fixed_ccols].drop_duplicates(),
                new_group_df,
                on=fixed_ccols,
                how='left',
            )

        length = len(ccols)
        for ccol, gcol in zip(ccols, old_group_df.columns[length:length * 2]):
            mapper = pd.Series(old_group_df[gcol].tolist(), index=old_group_df[ccol].tolist()).to_dict()
            new_group_df[gcol] = new_group_df[gcol].replace(mapper)
        return new_group_df


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

    @staticmethod
    def merge_groups(old_group_df: pd.DataFrame,
                     new_group_df: pd.DataFrame,
                     ccols: list[str],
                     fixed_ccols: list[str]
                     ) -> pd.DataFrame:
        if len(fixed_ccols):
            new_group_df = pd.merge(
                old_group_df[fixed_ccols].drop_duplicates(),
                new_group_df,
                on=fixed_ccols,
                how='left',
            )

        length = len(ccols)
        for ccol, gcol in zip(ccols * 2, old_group_df.columns[length:length * 4]):
            mapper = pd.Series(old_group_df[gcol].tolist(), index=old_group_df[ccol].tolist()).to_dict()
            new_group_df[gcol] = new_group_df[gcol].replace(mapper)

        return new_group_df
