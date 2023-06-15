import loguru
import pydantic
from sqlalchemy import Table

from .. import db
from .. import core_types
from .. import models as core_models
from . import models as report_models
from . import schema, schema_output, enums


class GroupRepo:
    table = core_models.Group

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

    async def delete(self, data: schema.GroupDeleteForm) -> None:
        async with db.get_async_session() as session:
            delete = self.table.delete().where(self.table.c.id == data.id_)
            await session.execute(delete)
            await session.commit()


class IntervalRepo:
    table = report_models.Interval

    async def create(self, data: schema.ReportIntervalCreateForm) -> core_types.Id_:
        async with db.get_async_session() as session:
            insert = self.table.insert().values(**data.dict()).returning(self.table.c.id)
            result = await session.execute(insert)
            await session.commit()
        result = result.fetchone()[0]
        return result


class ReportRepo:
    table_report = core_models.Report
    table_interval = report_models.Interval

    async def create(self, data: schema.ReportCreateForm) -> core_types.Id_:
        report_data = {
            "title": data.title,
            "category": enums.Category[data.category].value,
            "source_base": data.source_base,
            "group": data.group,
            "sheet": data.sheet,
        }

        async with db.get_async_session() as session:
            # Create report model
            insert = self.table_report.insert().values(report_data).returning(self.table_report.c.id)
            result = await session.execute(insert)
            report_id = result.fetchone()[0]

            # Create Interval model
            interval_data = data.interval.dict()
            interval_data['report'] = report_id
            insert = self.table_interval.insert().values(interval_data)
            _ = await session.execute(insert)

            await session.commit()

        return report_id

    async def retrieve(self):
        pass

    async def delete(self):
        pass

    async def retrieve_list(self):
        pass
