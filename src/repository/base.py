import pandas as pd
from loguru import logger
from sqlalchemy import Table, Column, Result
from sqlalchemy.ext.asyncio import AsyncSession

from .. import core_types
from . import db
from .service import helpers


class BaseRepo:
    table: Table

    async def create(self, data: dict) -> core_types.Id_:
        async with db.get_async_session() as session:
            insert = self.table.insert().values(**data).returning(self.table.c.id)
            result = await session.execute(insert)
            _ = await session.commit()
            return result.fetchone()[0]

    async def create_with_session(self, data: dict, session: AsyncSession) -> core_types.Id_:
        insert = self.table.insert().values(**data).returning(self.table.c.id)
        result = await session.execute(insert)
        return result.fetchone()[0]

    async def create_bulk_with_session(self, data: list[dict], session: AsyncSession,
                                       chunksize=10_000) -> list[core_types.Id_]:
        ids: list[core_types.Id_] = []

        splited = helpers.split_list(data, chunksize)
        for part in splited:
            insert = self.table.insert().values(part).returning(self.table.c.id)
            result = await session.execute(insert)
            result = result.fetchall()
            result = pd.Series(result).apply(lambda x: x[0]).tolist()
            ids.extend(result)

        return ids

    async def retrieve_and_delete(self, **kwargs) -> dict:
        raise NotImplemented

    async def retrieve_and_delete_with_session(self, session: AsyncSession, **kwargs) -> dict:
        delete = self.table.delete().filter_by(**kwargs).returning(self.table.columns)
        result = await session.execute(delete)
        result = Result.mappings(result).fetchall()

        if len(result) == 0:
            raise LookupError(f"there is not entity with following filter: {kwargs}")

        if len(result) > 1:
            raise LookupError(f"there are to many entities with following filter: {kwargs}. "
                              f"Change filter or use bulk method to delete many entities")
        result = dict(result[0])
        return result


