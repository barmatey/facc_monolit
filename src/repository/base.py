import pandas as pd
from loguru import logger
from sqlalchemy import Table, Result
from sqlalchemy.ext.asyncio import AsyncSession

from .service import helpers
from .. import core_types


class BaseRepo:
    table: Table

    async def _create(self, data: dict, session: AsyncSession, commit=False) -> core_types.Id_:
        insert = self.table.insert().values(**data).returning(self.table.c.id)
        result = await session.execute(insert)
        if commit:
            await session.commit()
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

    async def _retrieve(self, id_: core_types.Id_, session: AsyncSession) -> dict:
        q = self.table.select().where(self.table.c.id == id_)
        result = await session.execute(q)
        result = dict(Result.mappings(result).fetchone())
        return result

    async def _delete(self, id_: core_types.Id_, session: AsyncSession, commit=False) -> None:
        delete = self.table.delete().where(self.table.c.id == id_)
        _ = await session.execute(delete)
        if commit:
            await session.commit()
