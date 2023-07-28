import typing
from loguru import logger
from typing import TypeVar

import pandas as pd
from pydantic import BaseModel as PydanticModel
from sqlalchemy import insert, Result, delete, update, GenerativeSelect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncSession

from core_types import OrderBy, DTO
from src.report import entities
from src import core_types

Entity = TypeVar(
    'Entity',
    entities.Group,
    entities.Report
)


class BaseModel(AsyncAttrs, DeclarativeBase):

    @classmethod
    def get_columns(cls) -> list[str]:
        # noinspection PyTypeChecker
        return [str(col.key) for col in cls.__table__.columns]

    def to_entity(self, **kwargs) -> Entity:
        raise NotImplemented


ReturningEntity = typing.Literal["MODEL", "ENTITY", "DICT", "FRAME", "FIELD"] | None

Model = TypeVar(
    'Model',
    bound=BaseModel
)


class BasePostgres:
    model: typing.Type[Model] = NotImplemented

    def __init__(self, session: AsyncSession, ):
        self._session = session

    def _parse_filters(self, filter_by: dict) -> list:
        result = [self.model.__table__.c[key] == value for key, value in filter_by.items() if value is not None]
        return result

    def _parse_orders(self, order_by: OrderBy | None, asc: bool = True) -> list:
        if order_by is None:
            return [self.model.id.asc()]
        if isinstance(order_by, str):
            if asc:
                return [self.model.__table__.c[order_by].asc()]
            else:
                return [self.model.__table__.c[order_by].desc()]
        if asc:
            return [self.model.__table__.c[col].asc() for col in order_by]
        else:
            return [self.model.__table__.c[col].desc() for col in order_by]

    @staticmethod
    def _parse_dto(data: DTO | list[DTO]) -> dict | list[dict]:
        if isinstance(data, list):
            if len(data) == 0:
                raise ValueError
            if isinstance(data[0], PydanticModel):
                data = [d.__dict__ for d in data]
            return data
        if isinstance(data, PydanticModel):
            data = data.dict()
            return data
        if isinstance(data, dict):
            return data
        raise TypeError

    @staticmethod
    def _paginate(stmt: GenerativeSelect, paginate_from: int, paginate_to: int):
        if paginate_from is not None and paginate_to is not None:
            stmt = stmt.slice(paginate_from, paginate_to)
        return stmt

    async def create_one(self, data: DTO) -> Model:
        data = self._parse_dto(data)
        session = self._session

        model = self.model(**data)
        session.add(model)
        await session.flush()
        return model

    async def create_many(self, data: list[DTO]) -> None:
        session = self._session
        data = self._parse_dto(data)
        stmt = insert(self.model).returning(self.model)
        _: Result = await session.execute(stmt, data)

    async def get_one(self, filter_by: dict) -> Model:
        session = self._session
        filters = self._parse_filters(filter_by)
        stmt = select(self.model).where(*filters)
        result = await session.execute(stmt)
        result = result.scalars().fetchall()[0]
        return result

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                       slice_from: int = None, slice_to: int = None) -> list[Model]:
        session = self._session

        filters = self._parse_filters(filter_by)
        orders = self._parse_orders(order_by, asc)
        stmt = select(self.model).where(*filters).order_by(*orders)
        stmt = self._paginate(stmt, slice_from, slice_to)

        result = await session.execute(stmt)
        result = list(result.scalars().fetchall())
        return result

    async def get_many_as_frame(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                                slice_from: int = None, slice_to: int = None) -> pd.DataFrame:
        session = self._session

        filters = self._parse_filters(filter_by)
        orders = self._parse_orders(order_by, asc)
        stmt = select(self.model.__table__).where(*filters).order_by(*orders)
        stmt = self._paginate(stmt, slice_from, slice_to)

        result = await session.execute(stmt)
        result = pd.DataFrame.from_records(result.fetchall(), columns=self.model.get_columns())
        return result

    async def update_one(self, data: core_types.DTO, filter_by: dict) -> Model:
        session = self._session
        filters = self._parse_filters(filter_by)
        data = self._parse_dto(data)
        stmt = update(self.model).where(*filters).values(**data).returning(self.model)
        result: Result = await session.execute(stmt)
        models: list[Model] = list(result.scalars())
        return models[0]

    async def update_many(self, data: DTO, filter_by: dict) -> None:
        data = self._parse_dto(data)
        filters = self._parse_filters(filter_by)
        stmt = update(self.model).where(*filters).values(**data).returning(self.model)
        _: Result = await self._session.execute(stmt)

    async def delete_one(self, filter_by: dict) -> Model:
        session = self._session
        filters = self._parse_filters(filter_by)
        stmt = delete(self.model).where(*filters).returning(self.model)
        result: Result = await session.execute(stmt)
        models: list[Model] = list(result.scalars())
        return models[0]

    async def delete_many(self, filter_by: dict) -> None:
        session = self._session
        filters = self._parse_filters(filter_by)
        stmt = delete(self.model).where(*filters)
        _: Result = await session.execute(stmt)
