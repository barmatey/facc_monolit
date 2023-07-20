import typing

import pandas as pd
from sqlalchemy import Integer, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

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

OrderBy = typing.Union[str, list[str]]


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

    def to_entity(self, interval: e_report.Interval) -> e_report.Report:
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
                readonly_all_cells=data.sheet.readonly_all_cells,
            )
            sheet_id = await self.sheet_repo().create_with_session(session, sheet_data)

            # Create interval model
            interval: Interval = await self.interval_repo().create_with_session(session, data.interval)

            # Create report model
            report_data = dict(
                title=data.title,
                category_id=CategoryEnum[data.category].value,
                source_id=data.source_id,
                group_id=data.group_id,
                interval_id=interval.id,
                sheet_id=sheet_id,
            )
            report: Report = await self.create_with_session(session, report_data)
            session.expunge_all()
            await session.commit()
            return report.to_entity(interval.to_entity())

    async def retrieve(self, filter_by: dict):
        async with db.get_async_session() as session:
            report: Report = await self.retrieve_with_session(session, filter_by)
            interval: Interval = await self.interval_repo().retrieve_with_session(session, {"id": report.interval_id})
            return report.to_entity(interval.to_entity())

    async def retrieve_bulk(self,
                            filter_by: dict,
                            order_by: OrderBy = None,
                            ascending: bool = True,
                            paginate_from: int = None,
                            paginate_to: int = None) -> list[e_report.Report]:
        async with db.get_async_session() as session:
            report: pd.DataFrame = await self.retrieve_bulk_as_dataframe_with_session(session, filter_by, order_by)
            interval: pd.DataFrame = await self.interval_repo().retrieve_bulk_as_dataframe_with_session(session, {})

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

    async def overwrite_linked_sheet(self, instance: e_report.Report, data: e_report.SheetCreate) -> None:
        async with db.get_async_session() as session:
            sheet_data = e_sheet.SheetCreate(
                df=data.dataframe,
                drop_index=data.drop_index,
                drop_columns=data.drop_columns,
                readonly_all_cells=data.readonly_all_cells
            )
            await self.sheet_repo().overwrite_with_session(session, instance.sheet_id, sheet_data)
            await session.commit()
