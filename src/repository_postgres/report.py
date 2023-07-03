import loguru
import pandas as pd
from sqlalchemy import Integer, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from src import core_types
from src.report import entities as e_report
from src.sheet import entities as e_sheet

from . import db
from .category import Category, CategoryEnum
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

    def to_report_entity(self, interval: e_report.Interval) -> e_report.Report:
        return e_report.Report(
            id=self.id,
            category=CategoryEnum(self.category_id).name,
            title=self.title,
            group_id=self.group_id,
            source_id=self.source_id,
            sheet_id=self.sheet_id,
            interval=interval.copy()
        )


class ReportRepo(BaseRepo):
    model = Report
    sheet_repo = SheetRepo
    interval_repo = IntervalRepo

    async def create(self, data: e_report.ReportCreate) -> e_report.Report:
        async with db.get_async_session() as session:
            # Create sheet model
            sheet_data = e_sheet.SheetCreate(
                df=data.sheet.dataframe,
                drop_index=data.sheet.drop_index,
                drop_columns=data.sheet.drop_columns,
            )
            sheet_id = await self.sheet_repo()._create_with_session(session, sheet_data)

            # Create interval model
            interval: Interval = await self.interval_repo()._create_with_session(session, data.interval.dict())

            # Create report model
            report_data = dict(
                title=data.title,
                category_id=CategoryEnum[data.category].value,
                source_id=data.source_id,
                group_id=data.group_id,
                interval_id=interval.id,
                sheet_id=sheet_id,
            )
            report: Report = await self._create_with_session(session, report_data)
            session.expunge_all()
            await session.commit()
            return report.to_report_entity(interval.to_interval_entity())

    async def retrieve_by_id(self, id_: core_types.Id_) -> e_report.Report:
        async with db.get_async_session() as session:
            report: Report = await super()._retrieve_with_session(session, {"id": id_})
            interval: Interval = await self.interval_repo()._retrieve_with_session(session, {"id": report.interval_id})
            return report.to_report_entity(interval.to_interval_entity())

    async def retrieve_bulk(self, filter_: dict = None, sort_by: str = None, ascending=True) -> list[e_report.Report]:
        if filter_ is None:
            filter_ = {}

        async with db.get_async_session() as session:
            report = await super()._retrieve_bulk_as_dataframe(session, filter_, sort_by, ascending)
            interval = await self.interval_repo()._retrieve_bulk_as_dataframe(session, {})

            report = pd.merge(
                report,
                interval.rename({'id': 'interval_id'}, axis=1),
                on='interval_id',
            )

            report_entities: list[e_report.Report] = []

            for i, row in report.iterrows():
                report_interval = e_report.Interval(
                    id=row['interval_id'],
                    total_start_date=row['total_start_date'],
                    total_end_date=row['total_end_date'],
                    start_date=row['start_date'],
                    end_date=row['end_date'],
                    period_year=row['period_year'],
                    period_month=row['period_month'],
                    period_day=row['period_day'],
                )
                report_entity = e_report.Report(
                    id=row['id'],
                    title=row['title'],
                    category=CategoryEnum(row['category_id']).name,
                    source_id=row['source_id'],
                    group_id=row['group_id'],
                    interval=report_interval,
                    sheet_id=row['sheet_id'],
                )
                report_entities.append(report_entity)

            return report_entities
