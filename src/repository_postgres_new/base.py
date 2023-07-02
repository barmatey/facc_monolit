import typing
from typing import TypeVar

import loguru
import pandas as pd
from pydantic import BaseModel as PydanticModel
from sqlalchemy import insert, select, Result, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from report import entities
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

    def to_entity(self) -> Entity:
        raise NotImplemented


OrderBy = typing.Union[str, list[str]]
DTO = typing.Union[PydanticModel, dict]


class BaseWithSession:
    model: typing.Type[BaseModel]

    async def _create_with_session(self, session: AsyncSession, data: DTO) -> Entity:
        if isinstance(data, PydanticModel):
            data = data.dict()
        model = self.model(**data)
        session.add(model)
        await session.flush()
        return model.to_entity()

    async def _create_bulk_with_session(self, session: AsyncSession, data: list[DTO]) -> list[core_types.Id_]:
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

    async def _retrieve_with_session(self, session: AsyncSession, filter_by: dict) -> Entity:
        stmt = (
            select(self.model)
            .filter_by(**filter_by)
        )
        result: Result = await session.execute(stmt)
        rows = result.scalars().fetchall()

        if len(rows) == 0:
            raise LookupError(f"there is no model with filter_={filter_by}")

        if len(rows) > 1:
            raise LookupError(f"there are to many objects with the {filter_by}; count objects: {len(rows)}")

        model: BaseModel = rows[0]
        return model.to_entity()

    async def _retrieve_bulk_with_session(self, session: AsyncSession,
                                          filter_by: dict, order_by: OrderBy = None) -> list[Entity]:

        sorter = await self._get_sorter(order_by)
        stmt = select(self.model).filter_by(**filter_by).order_by(*sorter)
        result: Result = await session.execute(stmt)
        models = result.scalars().fetchall()
        entity_list: list[Entity] = [model.to_entity() for model in models]
        return entity_list

    async def _retrieve_bulk_as_records_with_session(self, session: AsyncSession,
                                                     filter_by: dict, order_by: OrderBy = None) -> list[tuple]:
        sorter = await self._get_sorter(order_by)
        stmt = select(self.model.__table__).filter_by(**filter_by).order_by(*sorter)
        result: Result = await session.execute(stmt)
        records = list(result)
        return records

    async def _retrieve_bulk_as_dicts_with_session(self, session: AsyncSession,
                                                   filter_by: dict, order_by: OrderBy = None) -> list[dict]:
        sorter = await self._get_sorter(order_by)
        stmt = select(self.model.__table__).filter_by(**filter_by).order_by(*sorter)
        result: Result = await session.execute(stmt)
        dicts = list(Result.mappings(result))
        return dicts

    async def _retrieve_bulk_as_dataframe_with_session(self, session: AsyncSession,
                                                       filter_by: dict, order_by: OrderBy = None) -> pd.DataFrame:
        sorter = await self._get_sorter(order_by)
        stmt = select(self.model.__table__).filter_by(**filter_by).order_by(*sorter)
        result: Result = await session.execute(stmt)
        df = pd.DataFrame.from_records(result, columns=self.model.get_columns())
        return df

    async def _delete_with_session(self, session: AsyncSession, filter_: dict) -> core_types.Id_:
        result = await session.execute(
            delete(self.model)
            .filter_by(**filter_)
            .returning(self.model.id)
        )

        result = list(result.scalars())
        if len(result) == 0:
            raise LookupError(f"there is not model with following filter: {filter_}")

        if len(result) > 1:
            raise LookupError(f"there are to many models with following filter: {filter_}. "
                              f"Change filter or use bulk method to delete many models")
        return result[0]

    async def _get_sorter(self, order_by: OrderBy | None) -> list:
        if order_by is None:
            return [self.model.id.asc()]
        if isinstance(order_by, str):
            return [self.model.__table__.c[order_by].asc()]
        return [self.model.__table__.c[col].asc() for col in order_by]


class BaseRepo(BaseWithSession):
    model: BaseModel

    async def create(self, data: DTO) -> Entity:
        async with db.get_async_session() as session:
            entity = await super()._create_with_session(session, data)
            await session.commit()
            return entity

    async def create_bulk(self, data: list[DTO]) -> list[core_types.Id_]:
        async with db.get_async_session() as session:
            result = await super()._create_bulk_with_session(session, data)
            await session.commit()
            return result

    async def retrieve(self, filter_by) -> Entity:
        async with db.get_async_session() as session:
            return await super()._retrieve_with_session(session, filter_by)

    async def retrieve_bulk(self, filter_by: dict, order_by: OrderBy = None) -> list[Entity]:
        async with db.get_async_session() as session:
            return await super()._retrieve_bulk_with_session(session, filter_by)

    async def retrieve_bulk_as_dataframe(self, filter_by: dict, order_by: OrderBy = None) -> pd.DataFrame:
        async with db.get_async_session() as session:
            return await super()._retrieve_bulk_as_dataframe_with_session(session, filter_by, order_by)

    async def retrieve_bulk_as_records(self, filter_by: dict, order_by: OrderBy = None) -> list[tuple]:
        async with db.get_async_session() as session:
            return await super()._retrieve_bulk_as_records_with_session(session, filter_by, order_by)

    async def retrieve_bulk_as_dicts(self, filter_by: dict, order_by: OrderBy = None) -> list[dict]:
        async with db.get_async_session() as session:
            return await super()._retrieve_bulk_as_dicts_with_session(session, filter_by, order_by)

    async def partial_update(self, data: DTO, filter_by: dict) -> Entity:
        pass

    async def partial_update_bulk(self, data: list[DTO], filter_by: dict) -> list[core_types.Id_]:
        pass

    async def delete(self, filter_by: dict) -> core_types.Id_:
        async with db.get_async_session() as session:
            deleted_id = await super()._delete_with_session(session, filter_by)
            await session.commit()
            return deleted_id
