import pandas as pd
import pandera as pa
from pandera.typing import Series


class WireSchema(pa.DataFrameModel):
    date: Series[pd.Timestamp]
    sender: Series[float]
    receiver: Series[float]
    debit: Series[float]
    credit: Series[float]
