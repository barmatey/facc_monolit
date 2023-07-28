from datetime import datetime

import pandas as pd
import pandera as pa
import typing

from sqlalchemy import TIMESTAMP, Float, String, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from src import core_types
from src.report.repository import WireRepo
from wire.entities import Wire

from .base import BasePostgres, BaseModel
from .source import SourceModel


class WireModel(BaseModel):
    __tablename__ = "wire"
    date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    sender: Mapped[float] = mapped_column(Float, nullable=False)
    receiver: Mapped[float] = mapped_column(Float, nullable=False)
    debit: Mapped[float] = mapped_column(Float, nullable=False)
    credit: Mapped[float] = mapped_column(Float, nullable=False)
    subconto_first: Mapped[str] = mapped_column(String(800), nullable=True)
    subconto_second: Mapped[str] = mapped_column(String(800), nullable=True)
    comment: Mapped[str] = mapped_column(String(800), nullable=True)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey(SourceModel.id, ondelete='CASCADE'), nullable=False)

    def to_entity(self, **kwargs) -> Wire:
        return Wire(
            id=self.id,
            date=self.date,
            sender=self.sender,
            receiver=self.receiver,
            debit=self.debit,
            credit=self.credit,
            subconto_first=self.subconto_first,
            subconto_second=self.subconto_second,
            comment=self.comment,
            source_id=self.source_id,
        )


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
