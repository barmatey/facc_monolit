from sqlalchemy import Table, Column, Integer, ForeignKey, String, MetaData

from .. import core_types
from ..report import entities as e_report
from ..sheet import entities as e_sheet
from . import db
from .category import Category
from .group import Group
from .source import SourceBase
from .base import BaseRepo
from .sheet import SheetRepo
from .interval import IntervalRepo

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


class ReportRepo(BaseRepo):
    table_report = Report
    sheet_repo = SheetRepo
    interval_repo = IntervalRepo

    async def create(self, report: e_report.ReportCreate, interval: e_report.ReportIntervalCreate) -> core_types.Id_:
        async with db.get_async_session() as session:
            # Create sheet first because report has foreign key "sheet_id"
            sheet_data = e_sheet.SheetCreate()
            _sheet_id = self.sheet_repo().create(sheet_data)

            # Create report model
            report_data = report.dict()
            report_id = await super()._create(report_data, session, commit=False)

            # Create interval
            interval_data = interval.dict()
            interval_data['report_id'] = report_id
            _ = await super(self.interval_repo, self.interval_repo())._create(interval_data, session, commit=False)

            return report_id

    async def retrieve(self, id_: core_types.Id_) -> e_report.Report:
        pass

    async def delete(self):
        pass

    async def retrieve_list(self):
        pass
