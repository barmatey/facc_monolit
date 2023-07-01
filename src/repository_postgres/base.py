import typing

import loguru
import pandas as pd
from sqlalchemy import select, insert, delete, update, desc, asc, Result, MappingResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src import core_types
from . import db


class BaseModel(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    @classmethod
    def get_columns(cls) -> list[str]:
        # noinspection PyTypeChecker
        return [str(col.key) for col in cls.__table__.columns]


class BaseRepo:
    model: typing.Type[BaseModel]

    def get_model(self):
        return self.model

    async def create(self, data: dict) -> BaseModel:
        async with db.get_async_session() as session:
            model = await self._create_with_session(session, data)
            session.expunge(model)
            await session.commit()
            return model

    async def _create_with_session(self, session: AsyncSession, data: dict) -> BaseModel:
        loguru.logger.warning(f'\n{data}')
        model = self.model(**data)
        session.add(model)
        await session.flush()
        return model

    async def create_bulk(self, data: list[dict]) -> list[core_types.Id_]:
        async with db.get_async_session() as session:
            result = await self._create_bulk_with_session(session, data)
            await session.commit()
            return result

    async def _create_bulk_with_session(self, session: AsyncSession, data: list[dict]) -> list[core_types.Id_]:
        # noinspection PyTypeChecker
        result = await session.scalars(
            insert(self.model)
            .returning(self.model.id),
            data
        )
        result = list(result)
        return result

    async def retrieve(self, filter_: dict) -> BaseModel:
        async with db.get_async_session() as session:
            return await self._retrieve_with_session(session, filter_)

    async def _retrieve_with_session(self, session: AsyncSession, filter_: dict) -> BaseModel:
        result = await session.execute(
            select(self.model)
            .filter_by(**filter_)
            .order_by()
        )
        result = result.fetchone()
        if result is None:
            raise LookupError(f"there is no model with filter_={filter_}")
        return result[0]

    async def retrieve_bulk(self, filter_: dict, sort_by: str = None, ascending=True) -> list[BaseModel]:
        async with db.get_async_session() as session:
            return await self._retrieve_bulk_with_session(session, filter_, sort_by, ascending)

    async def _retrieve_bulk_with_session(self, session: AsyncSession,
                                          filter_: dict, sort_by: str = None, ascending=True) -> list[BaseModel]:
        if sort_by is not None:
            sorter = asc(sort_by) if ascending else desc(sort_by)
        else:
            sorter = asc(self.model.id.key)

        result = await session.scalars(
            select(self.model)
            .filter_by(**filter_)
            .order_by(sorter)
        )
        result = list(result)
        return result

    async def _retrieve_bulk_as_result(self, session: AsyncSession, filter_: dict,
                                       sort_by: str = None, ascending=True) -> Result:
        if sort_by is not None:
            sorter = asc(sort_by) if ascending else desc(sort_by)
        else:
            sorter = asc(self.model.id.key)

        result = await session.execute(
            select(self.model.__table__)
            .filter_by(**filter_)
            .order_by(sorter)
        )
        return result

    async def _retrieve_bulk_as_records(self, session: AsyncSession, filter_: dict,
                                        sort_by: str = None, ascending=True) -> list[tuple]:
        result = await self._retrieve_bulk_as_result(session, filter_, sort_by, ascending)
        result = list(result)
        return result

    async def _retrieve_bulk_as_dataframe(self, session: AsyncSession, filter_: dict,
                                          sort_by: str = None, ascending=True) -> pd.DataFrame:
        result = await self._retrieve_bulk_as_result(session, filter_, sort_by, ascending)
        df = pd.DataFrame.from_records(result, columns=self.model.get_columns())
        return df

    async def _retrieve_bulk_as_dicts(self, session: AsyncSession, filter_: dict,
                                      sort_by: str = None, ascending=True) -> list[dict]:
        result = await self._retrieve_bulk_as_result(session, filter_, sort_by, ascending)
        result = Result.mappings(result)
        return list(result)

    async def _update_with_session(self, session: AsyncSession, filter_: dict, data: dict) -> None:
        stmt = (
            update(self.model)
            .filter_by(**filter_)
            .values(**data)
        )
        _ = await session.execute(stmt)

    async def delete(self, filter_: dict) -> None:
        async with db.get_async_session() as session:
            await self._delete_with_session(session, filter_)
            await session.commit()

    async def _delete_with_session(self, session: AsyncSession, filter_: dict) -> None:
        result = await session.execute(
            delete(self.model)
            .filter_by(**filter_)
            .returning(self.model.id)
        )

        result = list(result)
        if len(result) == 0:
            raise LookupError(f"there is not model with following filter: {filter_}")

        if len(result) > 1:
            raise LookupError(f"there are to many models with following filter: {filter_}. "
                              f"Change filter or use bulk method to delete many models")

    async def retrieve_and_delete(self, filter_: dict) -> BaseModel:
        async with db.get_async_session() as session:
            result = await self._retrieve_and_delete_with_session(session, filter_)
            await session.commit()
            return result

    async def _retrieve_and_delete_with_session(self, session: AsyncSession, filter_: dict) -> BaseModel:
        result = await session.scalars(
            delete(self.model)
            .filter_by(**filter_)
            .returning(self.model)
        )
        result = list(result)
        if len(result) == 0:
            raise LookupError(f"there is not model with following filter: {filter_}")

        if len(result) > 1:
            raise LookupError(f"there are to many models with following filter: {filter_}. "
                              f"Change filter or use bulk method to delete many models")
        result = result[0]
        return result
