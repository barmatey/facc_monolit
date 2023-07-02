import typing
from typing import TypeVar

import loguru
import pandas as pd
from pydantic import BaseModel as PydanticModel
from sqlalchemy import insert, select, Result
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from report import entities
from report.repository_new import Repository
from src import core_types
from . import db

Entity = TypeVar(
    'Entity',
    entities.Group,
    entities.Report
)


class BaseModel(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    @classmethod
    def get_columns(cls) -> list[str]:
        # noinspection PyTypeChecker
        return [str(col.key) for col in cls.__table__.columns]

    @classmethod
    def to_entity(cls) -> Entity:
        raise NotImplemented


class BaseWithSession:
    model: typing.Type[BaseModel]

    async def _create_with_session(self, session: AsyncSession, data: PydanticModel | dict) -> Entity:
        if isinstance(data, PydanticModel):
            data = data.dict()
        model = self.model(**data)
        session.add(model)
        await session.flush()
        return model.to_entity()

    async def _create_bulk_with_session(self, session: AsyncSession, data: list[dict] | list[PydanticModel]
                                        ) -> list[core_types.Id_]:
        if len(data) == 0:
            raise ValueError
        if isinstance(data[0], PydanticModel):
            data = [d.__dict__ for d in data]

        result = await session.execute(
            insert(self.model)
            .returning(self.model.id),
            data
        )
        ids = list(result.scalars())
        return ids

    async def _retrieve_with_session(self, session: AsyncSession, **kwargs) -> Entity:
        stmt = (
            select(self.model)
            .filter_by(**kwargs)
        )
        result: Result = await session.execute(stmt)
        rows = result.scalars().fetchall()

        if len(rows) == 0:
            raise LookupError(f"there is no model with filter_={kwargs}")

        if len(rows) > 1:
            raise LookupError(f"there are to many objects with the {kwargs}; count objects: {len(rows)}")

        model: BaseModel = rows[0]
        return model.to_entity()


class BaseRepo(Repository, BaseWithSession):
    model: BaseModel

    async def create(self, data: PydanticModel) -> Entity:
        async with db.get_async_session() as session:
            entity = await self._create_with_session(session, data)
            await session.commit()
            return entity

    async def create_bulk(self, data: list[PydanticModel] | list[dict]) -> list[core_types.Id_]:
        async with db.get_async_session() as session:
            result = await self._create_bulk_with_session(session, data)
            await session.commit()
            return result

    async def retrieve(self, **kwargs) -> Entity:
        async with db.get_async_session() as session:
            return await self._retrieve_with_session(session, **kwargs)

    async def retrieve_bulk(self, **kwargs) -> list[Entity]:
        pass

    async def retrieve_bulk_as_dataframe(self, **kwargs) -> pd.DataFrame:
        pass

    async def partial_update(self, data: PydanticModel, **kwargs) -> Entity:
        pass

    async def partial_update_bulk(self, data: list[PydanticModel] | list[dict], **kwargs) -> list[core_types.Id_]:
        pass

    async def delete(self, **kwargs) -> core_types.Id_:
        pass
