import typing
from copy import deepcopy

import pandas as pd
import pandera as pa


# todo need date validation
class WireSchema(pa.DataFrameModel):
    date: pa.typing.Series[typing.Any]
    sender: pa.typing.Series[float]
    receiver: pa.typing.Series[float]
    debit: pa.typing.Series[float]
    credit: pa.typing.Series[float]


class Wire:
    def __init__(self, wire_df: pd.DataFrame):
        WireSchema.validate(wire_df)
        if wire_df.isna().sum().sum() > 0:
            raise ValueError('wire_df count NaNs > 0')
        self.wire_df = wire_df.copy()

    def get_wire_df(self) -> pd.DataFrame:
        return self.wire_df

    def copy(self) -> typing.Self:
        return deepcopy(self)