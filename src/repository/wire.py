import math
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame
from sqlalchemy import Table, Column, Integer, ForeignKey, String, MetaData, TIMESTAMP, Float
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from datetime import datetime

from .. import core_types
from . import db
from .service import helpers
from .base import BaseRepo, BaseModel
from .source import Source


class Wire(BaseModel):
    __tablename__ = "wire"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False)
    sender: Mapped[float] = mapped_column(Float, nullable=False)
    receiver: Mapped[float] = mapped_column(Float, nullable=False)
    debit: Mapped[float] = mapped_column(Float, nullable=False)
    credit: Mapped[float] = mapped_column(Float, nullable=False)
    subconto_first: Mapped[str] = mapped_column(String(800), nullable=True)
    subconto_second: Mapped[str] = mapped_column(String(800), nullable=True)
    comment: Mapped[str] = mapped_column(String(800), nullable=True)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey(Source.id, ondelete='CASCADE'), nullable=False)


class WireSchema(pa.DataFrameModel):
    source_id: pa.typing.Series[core_types.Id_]
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
        df = df[['source_id', 'date', 'sender', 'receiver', 'debit', 'credit', 'subconto_first', 'subconto_second',
                 'comment']]
        return df


class WireRepo(BaseRepo):
    table = Wire

    async def bulk_create_wire(self, wires: DataFrame[WireSchema]) -> None:
        WireSchema.validate(wires)
        wires = await WireSchema.drop_extra_columns(wires)

        chunksize = math.ceil(10_000 / len(wires.columns))
        splited = helpers.split_dataframe(wires, chunksize)

        async with db.get_async_session() as session:
            for part in splited:
                insert = self.table.insert().values(part.to_dict(orient='records'))
                await session.execute(insert)
            await session.commit()

    # todo обработать запрос с несуществующим sheet_id
    async def retrieve_wire_df(self, source_id: core_types.Id_) -> DataFrame[WireSchema]:
        async with db.get_async_session() as session:
            select = self.table.select().where(self.table.c.source_id == source_id)
            result = await session.execute(select)
            df: DataFrame[WireSchema] = pd.DataFrame(result.fetchall())
            WireSchema.validate(df)
            df = df[['date', 'sender', 'receiver', 'debit', 'credit', 'subconto_first', 'subconto_second', 'comment']]
            return df
