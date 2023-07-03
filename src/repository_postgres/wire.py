import pandas as pd
import pandera as pa
from sqlalchemy import Integer, ForeignKey, String, TIMESTAMP, Float
from sqlalchemy.orm import Mapped, mapped_column

from datetime import datetime

from src import core_types
from src.wire import entities
from . import db
from .base import BaseRepo, BaseModel
from .source import Source, SourceRepo


class Wire(BaseModel):
    __tablename__ = "wire"
    date: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False)
    sender: Mapped[float] = mapped_column(Float, nullable=False)
    receiver: Mapped[float] = mapped_column(Float, nullable=False)
    debit: Mapped[float] = mapped_column(Float, nullable=False)
    credit: Mapped[float] = mapped_column(Float, nullable=False)
    subconto_first: Mapped[str] = mapped_column(String(800), nullable=True)
    subconto_second: Mapped[str] = mapped_column(String(800), nullable=True)
    comment: Mapped[str] = mapped_column(String(800), nullable=True)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey(Source.id, ondelete='CASCADE'), nullable=False)

    def to_entity(self, **kwargs):
        raise NotImplemented


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
    model = Wire
    source_repo = SourceRepo

    async def create_bulk(self, data: list[entities.WireCreate]) -> list[core_types.Id_]:
        raise NotImplemented

    async def bulk_create_wire(self, source_id: core_types.Id_, wires: pd.DataFrame) -> None:
        wires = wires.copy()
        wires['source_id'] = source_id
        WireSchema.validate(wires)
        wires = await WireSchema.drop_extra_columns(wires)
        data_for_source_update = {
            "total_start_date": wires['date'].min(),
            "total_end_date": wires['date'].max(),
        }
        async with db.get_async_session() as session:
            _ = await self.create_bulk_with_session(session, wires.to_dict(orient='records'))
            _ = await self.source_repo().update_with_session(session, {"id": source_id}, data_for_source_update)
            await session.commit()

    async def retrieve_bulk_as_dataframe(self, filter_by: dict, order_by: core_types.OrderBy = None) -> pd.DataFrame:
        source_id = filter_by['source_id']

        wire_df = await super().retrieve_bulk_as_dataframe(filter_by, order_by)
        if len(wire_df) == 0:
            raise LookupError(f'wires with source_id={source_id} is not found')

        WireSchema.validate(wire_df)
        wire_df = wire_df[['date', 'sender', 'receiver', 'debit', 'credit', 'subconto_first', 'subconto_second', 'comment']]
        return wire_df

