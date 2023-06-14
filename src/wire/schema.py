import pandas as pd
import pydantic
import pandera as pa


class CreateSourceForm(pydantic.BaseModel):
    title: str


class WireSchema(pa.DataFrameModel):
    date: pa.typing.Series[pd.Timestamp]
    sender: pa.typing.Series[float]
    receiver: pa.typing.Series[float]
    debit: pa.typing.Series[float]
    credit: pa.typing.Series[float]
    subconto_first: pa.typing.Series[str] = pa.Field(str_length={'max_value': 800})
    subconto_second: pa.typing.Series[str] = pa.Field(str_length={'max_value': 800})
    comment: pa.typing.Series[str] = pa.Field(str_length={'max_value': 800})

    @classmethod
    async def drop_extra_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[['date', 'sender', 'receiver', 'debit', 'credit', 'subconto_first', 'subconto_second', 'comment']]
        return df
