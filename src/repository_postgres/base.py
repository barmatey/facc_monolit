import typing
from typing import TypeVar

import loguru
import pandas as pd
from pydantic import BaseModel as PydanticModel
from sqlalchemy import insert, select, Result, delete, update, GenerativeSelect
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.report import entities
from src import core_types
from src.repository_postgres import db

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

    def to_entity(self, **kwargs) -> Entity:
        raise NotImplemented


OrderBy = typing.Union[str, list[str]]
DTO = typing.Union[PydanticModel, dict]

Model = TypeVar(
    'Model',
    bound=BaseModel
)


class BaseWithSession:
    model: typing.Type[BaseModel]

    async def create_with_session(self, session: AsyncSession, data: DTO) -> Model:
        if isinstance(data, PydanticModel):
            data = data.dict()
        model = self.model(**data)
        session.add(model)
        await session.flush()
        return model

    async def create_bulk_with_session(self, session: AsyncSession, data: list[DTO]) -> list[core_types.Id_]:
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

    async def retrieve_with_session(self, session: AsyncSession, filter_by: dict) -> Model:
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
        return model

    async def retrieve_bulk_with_session(self, session: AsyncSession,
                                         filter_by: dict,
                                         order_by: OrderBy = None,
                                         ascending: bool = True,
                                         paginate_from: int = None,
                                         paginate_to: int = None) -> list[Model]:
        filter_by = await self._dropna_from_filter(filter_by)
        sorter = await self._get_sorter(order_by)
        stmt = select(self.model).filter_by(**filter_by).order_by(*sorter)
        stmt = await self._paginate(stmt, paginate_from, paginate_to)
        result: Result = await session.execute(stmt)
        models = list(result.scalars().fetchall())
        return models

    async def retrieve_bulk_as_records_with_session(self, session: AsyncSession,
                                                    filter_by: dict, order_by: OrderBy = None) -> list[tuple]:
        sorter = await self._get_sorter(order_by)
        stmt = select(self.model.__table__).filter_by(**filter_by).order_by(*sorter)
        result: Result = await session.execute(stmt)
        records = list(result)
        return records

    async def retrieve_bulk_as_dicts_with_session(self, session: AsyncSession,
                                                  filter_by: dict, order_by: OrderBy = None) -> list[dict]:
        sorter = await self._get_sorter(order_by)
        stmt = select(self.model.__table__).filter_by(**filter_by).order_by(*sorter)
        result: Result = await session.execute(stmt)
        dicts = list(Result.mappings(result))
        return dicts

    async def retrieve_bulk_as_dataframe_with_session(self, session: AsyncSession,
                                                      filter_by: dict, order_by: OrderBy = None) -> pd.DataFrame:
        sorter = await self._get_sorter(order_by)
        stmt = select(self.model.__table__).filter_by(**filter_by).order_by(*sorter)
        result: Result = await session.execute(stmt)
        df = pd.DataFrame.from_records(result.fetchall(), columns=self.model.get_columns())
        return df

    async def update_with_session(self, session: AsyncSession, filter_by: dict, data: DTO) -> Model:
        if isinstance(data, PydanticModel):
            data = data.dict()
        data = {key: value for key, value in data.items() if value is not None}
        stmt = update(self.model).filter_by(**filter_by).values(**data).returning(self.model)
        result: Result = await session.execute(stmt)
        models: list[Model] = list(result.scalars())

        if len(models) == 0:
            raise LookupError(f"there is not model with following filter: {filter_by}")

        if len(models) > 1:
            raise LookupError(f"there are to many models with following filter: {filter_by}. "
                              f"Change filter or use bulk method to delete many models")
        return models.pop()

    async def delete_with_session(self, session: AsyncSession, filter_by: dict) -> core_types.Id_:
        result = await session.execute(
            delete(self.model)
            .filter_by(**filter_by)
            .returning(self.model.id)
        )

        result = list(result.scalars())
        if len(result) == 0:
            raise LookupError(f"there is not model with following filter: {filter_by}")

        if len(result) > 1:
            raise LookupError(f"there are to many models with following filter: {filter_by}. "
                              f"Change filter or use bulk method to delete many models")
        return result[0]

    async def _get_sorter(self, order_by: OrderBy | None) -> list:
        if order_by is None:
            return [self.model.id.asc()]
        if isinstance(order_by, str):
            return [self.model.__table__.c[order_by].asc()]
        return [self.model.__table__.c[col].asc() for col in order_by]

    @staticmethod
    async def _paginate(stmt: GenerativeSelect, paginate_from: int, paginate_to: int):
        if paginate_from is not None and paginate_to is not None:
            stmt = stmt.slice(paginate_from, paginate_to)
        return stmt

    @staticmethod
    async def _dropna_from_filter(filter_by: dict) -> dict:
        return {key: value for key, value in filter_by.items() if value is not None}


class BaseRepo(BaseWithSession):
    model: BaseModel

    async def create(self, data: DTO) -> Entity:
        async with db.get_async_session() as session:
            model = await super().create_with_session(session, data)
            entity = model.to_entity()
            await session.commit()
            return entity

    async def create_bulk(self, data: list[DTO]) -> list[core_types.Id_]:
        async with db.get_async_session() as session:
            result = await super().create_bulk_with_session(session, data)
            await session.commit()
            return result

    async def retrieve(self, filter_by) -> Entity:
        async with db.get_async_session() as session:
            model = await super().retrieve_with_session(session, filter_by)
            entity = model.to_entity()
            return entity

    async def retrieve_bulk(self,
                            filter_by: dict,
                            order_by: OrderBy = None,
                            ascending: bool = True,
                            paginate_from: int = None,
                            paginate_to: int = None) -> list[Entity]:
        async with db.get_async_session() as session:
            models = await super().retrieve_bulk_with_session(session, filter_by, order_by, ascending, paginate_from,
                                                              paginate_to)
            entity_list = [model.to_entity() for model in models]
            return entity_list

    async def retrieve_bulk_as_dataframe(self, filter_by: dict, order_by: OrderBy = None) -> pd.DataFrame:
        async with db.get_async_session() as session:
            return await super().retrieve_bulk_as_dataframe_with_session(session, filter_by, order_by)

    async def retrieve_bulk_as_records(self, filter_by: dict, order_by: OrderBy = None) -> list[tuple]:
        async with db.get_async_session() as session:
            return await super().retrieve_bulk_as_records_with_session(session, filter_by, order_by)

    async def retrieve_bulk_as_dicts(self, filter_by: dict, order_by: OrderBy = None) -> list[dict]:
        async with db.get_async_session() as session:
            return await super().retrieve_bulk_as_dicts_with_session(session, filter_by, order_by)

    async def update(self, data: DTO, filter_by: dict) -> Entity:
        async with db.get_async_session() as session:
            model = await super().update_with_session(session, filter_by, data)
            entity = model.to_entity()
            await session.commit()
            return entity

    async def update_bulk(self, data: list[DTO], filter_by: dict) -> list[core_types.Id_]:
        raise NotImplemented

    async def delete(self, filter_by: dict) -> core_types.Id_:
        async with db.get_async_session() as session:
            deleted_id = await super().delete_with_session(session, filter_by)
            await session.commit()
            return deleted_id
