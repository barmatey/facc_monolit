import pandas as pd
from sqlalchemy import insert, select, Result, update
from sqlalchemy.ext.asyncio import AsyncSession as AS
from sqlalchemy.orm import DeclarativeBase
import pydantic
from typing import Generic, Type, Callable

from .my_dto import DTO
from .my_filter import MyFilter
from .my_order import MyOrder, OrderBy
from .my_repo import Repository, Entity


class Model(DeclarativeBase):
    @classmethod
    def get_columns(cls) -> list[str]:
        # noinspection PyTypeChecker
        return [str(col.key) for col in cls.__table__.columns]

    def to_entity(self, **kwargs) -> Entity:
        raise NotImplemented


class RepositoryPostgres(Repository, Generic[Entity]):
    model: Type[Model]
    async_session_maker: Callable
    my_filter = MyFilter
    my_order = MyOrder

    async def create_one(self, data: DTO) -> Entity:
        async with self.async_session_maker() as session:
            model = await self.s_create_one(session, data)
            entity = model.to_entity()
            await session.commit()
            return entity

    async def create_many(self, data: list[DTO], without_return=False) -> list[Entity]:
        async with self.async_session_maker() as session:
            result = await self.s_create_many(session, data, without_return)
            await session.commit()
            return result

    async def get_one(self, filter_by: dict) -> Entity:
        async with self.async_session_maker() as session:
            model = await self.s_get_one(session, filter_by)
            entity = model.to_entity()
            return entity

    async def get_many(self, filter_by: dict, order_by: list, asc: bool = True) -> list[Entity]:
        async with self.async_session_maker() as session:
            models = await self.s_get_many(session, filter_by, order_by, asc)
            entity_list = [model.to_entity() for model in models]
            return entity_list

    async def get_many_as_frame(self, filter_by: dict, order_by: list, asc=True) -> pd.DataFrame:
        async with self.async_session_maker() as session:
            return await self.s_get_many_as_frame(session, filter_by, order_by)

    async def get_many_as_dicts(self, filter_by: dict) -> list[dict]:
        pass

    async def get_many_as_records(self, filter_by: dict) -> list[tuple]:
        pass

    async def update_one(self, data: DTO, filter_by: dict) -> Entity:
        async with self.async_session_maker() as session:
            model = await self.s_update_one(session, data, filter_by)
            entity = model.to_entity()
            await session.commit()
            return entity

    """With session"""

    async def s_create_one(self, session: AS, data: DTO) -> Model:
        if isinstance(data, pydantic.BaseModel):
            data = data.dict()
        model = self.model(**data)
        session.add(model)
        await session.flush()
        return model

    async def s_create_many(self, session: AS, data: list[DTO], without_return=False) -> list[Model]:
        if len(data) == 0:
            raise ValueError
        if isinstance(data[0], pydantic.BaseModel):
            data = [d.__dict__ for d in data]

        stmt = insert(self.model).returning(self.model)
        result = await session.execute(stmt, data)
        models = list(result.scalars()) if not without_return else []
        return models

    async def s_get_one(self, session: AS, filter_by: dict) -> Model:
        filters = self.my_filter(filter_by, self.model).get_filters()
        stmt = select(self.model).where(*filters)
        result: Result = await session.execute(stmt)
        rows = result.scalars().fetchall()

        if len(rows) == 0:
            raise LookupError(f"there is no model with filter_={filter_by}")

        if len(rows) > 1:
            raise LookupError(f"there are to many objects with the {filter_by}; count objects: {len(rows)}")

        model: Model = rows[0]
        return model

    async def s_get_many(self, session: AS, filter_by: dict, order_by: list = None, asc: bool = True) -> list[Model]:
        filters = self.my_filter(filter_by, self.model).get_filters()
        order = self.my_order(order_by, asc, self.model).get_sorter()
        stmt = select(self.model).where(*filters).order_by(*order)
        result: Result = await session.execute(stmt)
        models = list(result.scalars().fetchall())
        return models

    async def s_get_many_as_frame(self, session, filter_by: dict, order_by: list = None, asc=True) -> pd.DataFrame:
        sorter = self.my_order(order_by, asc, self.model).get_sorter()
        filters = self.my_filter(filter_by, self.model)
        stmt = select(self.model.__table__).where(*filters).order_by(*sorter)
        result: Result = await session.execute(stmt)
        df = pd.DataFrame.from_records(result.fetchall(), columns=self.model.get_columns())
        return df

    async def s_update_one(self, session: AS, data: DTO, filter_by: dict) -> Model:
        if isinstance(data, pydantic.BaseModel):
            data = data.dict()
        data = {key: value for key, value in data.items() if value is not None}
        filters = self.my_filter(filter_by, self.model)
        stmt = update(self.model).where(*filters).values(**data).returning(self.model)
        result: Result = await session.execute(stmt)
        models: list[Model] = list(result.scalars())

        if len(models) == 0:
            raise LookupError(f"there is not model with following filter: {filter_by}")

        if len(models) > 1:
            raise LookupError(f"there are to many models with following filter: {filter_by}. "
                              f"Change filter or use bulk method to delete many models")
        return models.pop()
