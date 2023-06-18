from sqlalchemy import Integer, ForeignKey, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .. import core_types
from ..report import entities as e_report
from ..sheet import entities as e_sheet
from . import db

from .category import Category
from .sheet import Sheet
from .group import Group
from .source import Source
from .interval import Interval, IntervalRepo

from .base import BaseRepo, BaseModel
from .sheet import SheetRepo


class Report(BaseModel):
    __tablename__ = 'report'
    title: Mapped[str] = mapped_column(String(80), nullable=False)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey(Category.id, ondelete='CASCADE'), nullable=False)
    group_id: Mapped[int] = mapped_column(Integer, ForeignKey(Group.id, ondelete='CASCADE'), nullable=False)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey(Source.id, ondelete='CASCADE'), nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False,
                                          unique=True)
    interval_id: Mapped[int] = mapped_column(Integer, ForeignKey(Interval.id, ondelete='RESTRICT'), nullable=False,
                                             unique=True)


class ReportRepo(BaseRepo):
    model = Report
    sheet_repo = SheetRepo
    interval_repo = IntervalRepo

    async def create(
            self,
            report: e_report.ReportCreate,
            interval: e_report.ReportIntervalCreate,
            sheet: e_sheet.SheetCreate,
    ) -> core_types.Id_:
        async with db.get_async_session() as session:
            # Create sheet
            sheet_id = await self.sheet_repo().create(sheet)

            # Create interval
            interval_data = interval.dict()
            interval_id = await super(self.interval_repo, self.interval_repo())._create(
                interval_data, session, commit=False)

            # Create report model
            report_data = report.dict()
            report_data['sheet_id'] = sheet_id
            report_data['interval_id'] = interval_id
            report_id = await super()._create(report_data, session, commit=False)

            # _ = await session.commit()
            return report_id

    async def retrieve(self, id_: core_types.Id_) -> e_report.Report:
        pass

    async def retrieve_list(self):
        pass
