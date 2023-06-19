from loguru import logger
from sqlalchemy import Integer, ForeignKey, String
from sqlalchemy.ext.asyncio import AsyncSession
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

    async def create(self, data: e_report.ReportCreate) -> core_types.Id_:
        async with db.get_async_session() as session:
            # Create sheet model
            sheet_data = e_sheet.SheetCreate(
                df=data.sheet.dataframe,
                drop_index=data.sheet.drop_index,
                drop_columns=data.sheet.drop_columns,
            )
            sheet_id = await self.sheet_repo().create_with_session(session, sheet_data)

            # Create interval model
            interval_id = await self.interval_repo().create_with_session(session, data.interval.dict())

            # Create report model
            report_data = dict(
                title=data.title,
                category_id=data.category.value,
                source_id=data.source_id,
                group_id=data.group_id,
                interval_id=interval_id,
                sheet_id=sheet_id,
            )
            report_id = await self.create_with_session(session, report_data)
            logger.debug(f"{report_id}")
            return report_id
