import loguru

from .. import db
from .. import core_types
from .. import models
from . import schema, schema_output, enums


class GroupRepo:
    table = models.Group

    async def create(self, data: schema.GroupCreateForm) -> core_types.Id_:
        data_dict = data.dict()
        data_dict['category'] = enums.Category[data.category].value
        async with db.get_async_session() as session:
            insert = self.table.insert().values(**data_dict).returning(self.table.c.id)
            result = await session.execute(insert)
            await session.commit()
        result = result.fetchone()[0]
        return result

    async def retrieve(self, data: schema.GroupRetrieveForm) -> schema_output.Group:
        async with db.get_async_session() as session:
            select = self.table.select().where(self.table.c.id == data.id_)
            cursor = await session.execute(select)
            result = {col.key: value for col, value in zip(self.table.columns, cursor.fetchone())}

        result = schema_output.Group(
            id_=result['id'],
            title=result['title'],
            category=enums.Category(result['category']).name,
            sheet=result['sheet'],
            source_base=result['source_base'],
        )
        return result


class ReportRepo:
    async def create_report(self):
        pass

    async def retrieve_report(self):
        pass

    async def delete_report(self):
        pass

    async def retrieve_report_list(self):
        pass
