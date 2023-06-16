from sqlalchemy import Table, Column, Integer, ForeignKey, String, MetaData

from .. import entities, core_types
from . import db
from .category import Category
from .group import Group
from .source import SourceBase

metadata = MetaData()

Report = Table(
    'report',
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String(80), nullable=False),
    Column("category_id", Integer, ForeignKey(Category.c.id, ondelete='CASCADE'), nullable=False),
    Column("group_id", Integer, ForeignKey(Group.c.id, ondelete='CASCADE'), nullable=False),
    Column("source_id", Integer, ForeignKey(SourceBase.c.id, ondelete='CASCADE'), nullable=False),
    Column("sheet_id", String(30), nullable=False, unique=True),
)


class ReportRepo:
    table_report = Report

    async def create(self, data: entities.Report) -> core_types.Id_:
        async with db.get_async_session() as session:
            insert = self.table_report.insert().values(**data.dict()).returning(self.table_report.c.id)
            result = await session.execute(insert)
            report_id = result.fetchone()[0]

            await session.commit()
            return report_id

    async def retrieve(self, id_: core_types.Id_) -> entities.Report:
        pass

    async def delete(self):
        pass

    async def retrieve_list(self):
        pass
