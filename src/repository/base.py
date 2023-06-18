import typing

import pandas as pd
from loguru import logger
from sqlalchemy import Result, select, insert, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .. import core_types
from . import db
from .service import helpers


class BaseModel(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


class BaseRepo:
    model: typing.Type[BaseModel]

    async def create(self, data: dict) -> core_types.Id_:
        async with db.get_async_session() as session:
            model = self.model(**data)
            session.add(model)
            await session.flush()
            _ = await session.commit()
            return model.id

    async def create_with_session(self, data: dict, session: AsyncSession) -> core_types.Id_:
        model = self.model(**data)
        session.add(model)
        await session.flush()
        return model.id

    async def create_bulk(self, data: list[dict]) -> list[core_types.Id_]:
        async with db.get_async_session() as session:
            result = await self.create_bulk_with_session(session, data)
            await session.commit()
            return result

    # noinspection PyTypeChecker
    async def create_bulk_with_session(self, session: AsyncSession, data: list[dict]) -> list[core_types.Id_]:
        result = await session.scalars(
            insert(self.model).returning(self.model.id),
            data
        )
        result = list(result)
        return result

    async def retrieve_bulk(self, filter_: dict) -> list[BaseModel]:
        async with db.get_async_session() as session:
            return await self.retrieve_bulk_with_session(session, filter_)

    async def retrieve_bulk_with_session(self, session: AsyncSession, filter_: dict) -> list[BaseModel]:
        result = await session.scalars(
            select(self.model).filter_by(**filter_)
        )
        result = list(result)
        return result

    async def delete_with_session(self, session: AsyncSession, filter_: dict) -> None:
        result = await session.execute(delete(self.model).filter_by(**filter_).returning(self.model.id))

        result = list(result)
        if len(result) == 0:
            raise LookupError(f"there is not model with following filter: {filter_}")

        if len(result) > 1:
            raise LookupError(f"there are to many models with following filter: {filter_}. "
                              f"Change filter or use bulk method to delete many models")

    async def retrieve_and_delete(self, filter_: dict) -> BaseModel:
        async with db.get_async_session() as session:
            result = await self.retrieve_and_delete_with_session(session, filter_)
            await session.commit()
            return result

    async def retrieve_and_delete_with_session(self, session: AsyncSession, filter_: dict) -> BaseModel:
        result = await session.scalars(
            delete(self.model).filter_by(**filter_).returning(self.model)
        )
        result = list(result)
        if len(result) == 0:
            raise LookupError(f"there is not model with following filter: {filter_}")

        if len(result) > 1:
            raise LookupError(f"there are to many models with following filter: {filter_}. "
                              f"Change filter or use bulk method to delete many models")
        result = result[0]
        return result
