import pandas as pd
import pandera as pa
import typing
from repository_postgres.wire import Wire as WireModel
from src import core_types
from src.report.repository import WireRepo

from .base import BasePostgres


class WireSchema(pa.DataFrameModel):
    source_id: pa.typing.Series[core_types.Id_]
    date: pa.typing.Series[typing.Any]
    sender: pa.typing.Series[float]
    receiver: pa.typing.Series[float]
    debit: pa.typing.Series[float]
    credit: pa.typing.Series[float]
    subconto_first: pa.typing.Series[str] = pa.Field(str_length={'max_value': 800})
    subconto_second: pa.typing.Series[str] = pa.Field(str_length={'max_value': 800})
    comment: pa.typing.Series[str] = pa.Field(str_length={'max_value': 800})

    @classmethod
    async def drop_extra_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[['source_id', 'date', 'sender', 'receiver', 'debit', 'credit', 'subconto_first', 'subconto_second',
                 'comment']]
        return df


class WireRepoPostgres(BasePostgres, WireRepo):
    model = WireModel

    async def get_wire_dataframe(self, filter_by: dict, order_by: core_types.OrderBy = None) -> pd.DataFrame:
        source_id = filter_by['source_id']

        wire_df = await super().get_many_as_frame(filter_by, order_by)
        if len(wire_df) == 0:
            raise LookupError(f'wires with source_id={source_id} is not found')

        WireSchema.validate(wire_df)
        wire_df = wire_df[['date', 'sender', 'receiver', 'debit', 'credit',
                           'subconto_first', 'subconto_second', 'comment']]
        return wire_df

