import loguru
from sqlalchemy import Table, inspect, Result
from sqlalchemy.ext.asyncio import AsyncSession

import core_types


class BaseRepo:
    table: Table

    async def _create(self, data: dict, session: AsyncSession, commit=True) -> core_types.Id_:
        insert = self.table.insert().values(**data).returning(self.table.c.id)
        result = await session.execute(insert)
        if commit:
            await session.commit()
        return result.fetchone()[0]

    async def _retrieve(self, id_: core_types.Id_, session: AsyncSession) -> dict:
        q = self.table.select().where(self.table.c.id == id_)
        result = await session.execute(q)
        result = dict(Result.mappings(result).fetchone())
        return result

