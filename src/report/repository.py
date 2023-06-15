import loguru
import pydantic
from sqlalchemy import Table

from .. import db
from .. import core_types
from .. import models
from . import schema, schema_output


class BaseRepo:
    table: Table

    async def create(self, data: pydantic.BaseModel) -> core_types.Id_:
        async with db.get_async_session() as session:
            insert = self.table.insert().values(**data.dict()).returning(self.table.c.id)
            result = await session.execute(insert)
            await session.commit()
            result = result.fetchone()[0]
            return result

    async def retrieve_group(self, data: schema.GroupRetrieveForm) -> schema_output.Group:
        pass

    async def delete_group(self, data: schema.GroupDeleteForm) -> None:
        pass

    async def retrieve_group_list(self) -> list[schema_output.Group]:
        pass


class GroupRepo(BaseRepo):
    table = models.Group

    async def create(self, data: schema.GroupCreateForm) -> core_types.Id_:
        return await super().create(data)


class ReportRepo:
    async def create_report(self):
        pass

    async def retrieve_report(self):
        pass

    async def delete_report(self):
        pass

    async def retrieve_report_list(self):
        pass
