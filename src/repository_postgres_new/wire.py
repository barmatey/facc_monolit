from datetime import datetime

import pandas as pd
import pandera as pa
import typing

from sqlalchemy import TIMESTAMP, Float, String, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from src.core_types import OrderBy, Id_, DTO
from src import core_types
from src.report.repository import WireRepo
from src.wire.repository import RepositoryCrud
from src.wire.entities import Wire

from .base import BasePostgres, BaseModel
from .source import SourceModel


class WireModel(BaseModel):
    __tablename__ = "wire"
    date: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    sender: Mapped[float] = mapped_column(Float, nullable=False)
    receiver: Mapped[float] = mapped_column(Float, nullable=False)
    debit: Mapped[float] = mapped_column(Float, nullable=False)
    credit: Mapped[float] = mapped_column(Float, nullable=False)
    sub1: Mapped[str] = mapped_column(String(800), nullable=True)
    sub2: Mapped[str] = mapped_column(String(800), nullable=True)
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
            sub1=self.sub1,
            sub2=self.sub2,
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
    sub1: pa.typing.Series[str] = pa.Field(str_length={'max_value': 800})
    sub2: pa.typing.Series[str] = pa.Field(str_length={'max_value': 800})
    comment: pa.typing.Series[str] = pa.Field(str_length={'max_value': 800})

    @classmethod
    async def drop_extra_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df[['source_id', 'date', 'sender', 'receiver', 'debit', 'credit', 'sub1', 'sub2', 'comment']]
        return df


class WireRepoPostgres(BasePostgres, WireRepo, RepositoryCrud):
    model = WireModel

    async def create_one(self, data: DTO) -> Wire:
        model: WireModel = await super().create_one(data)
        return model.to_entity()

    async def get_one(self, filter_by: dict) -> Wire:
        model: WireModel = await super().get_one(filter_by)
        return model.to_entity()

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True, slice_from: int = None,
                       slice_to: int = None):
        models: list[WireModel] = await super().get_many(filter_by, order_by, asc, slice_from, slice_to)
        entities = [x.to_entity() for x in models]
        return entities

    async def update_one(self, data: DTO, filter_by: dict) -> Wire:
        updated: WireModel = await super().update_one(data, filter_by)
        return updated.to_entity()

    async def delete_one(self, filter_by: dict) -> Wire:
        model: WireModel = await super().delete_one(filter_by)
        return model.to_entity()

    async def get_many_as_frame(self, filter_by: dict, order_by: OrderBy = None, asc=True, slice_from: int = None,
                                slice_to: int = None) -> pd.DataFrame:
        wire_df = await super().get_many_as_frame(filter_by, order_by)
        if len(wire_df) == 0:
            raise LookupError(f'wires with source_id={filter_by["source_id"]} is not found')

        WireSchema.validate(wire_df)
        wire_df = wire_df[['date', 'sender', 'receiver', 'debit', 'credit', 'sub1', 'sub2', 'comment']]
        return wire_df

    async def get_wire_dataframe(self, filter_by: dict, order_by: core_types.OrderBy = None) -> pd.DataFrame:
        return await self.get_many_as_frame(filter_by, order_by)
