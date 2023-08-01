import pandas as pd
from sqlalchemy import String, Integer, ForeignKey, TIMESTAMP, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from src.core_types import OrderBy, DTO, Id_
from src.report.entities import Report, Interval
from src.report.schema import ReportCreateSchema, FullReportCreateSchema
from src.sheet.schema import SheetCreateSchema

from src.report.repository import ReportRepo, Entity
from .base import BasePostgres, BaseModel
from .group import GroupModel
from .interval import IntervalRepoPostgres, IntervalModel
from .sheet import SheetRepoPostgres, SheetModel
from .category import CategoryEnum, CategoryModel
from .source import SourceModel


class ReportModel(BaseModel):
    __tablename__ = 'report'
    title: Mapped[str] = mapped_column(String(80), nullable=False)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey(CategoryModel.id, ondelete='CASCADE'), nullable=False)
    group_id: Mapped[int] = mapped_column(Integer, ForeignKey(GroupModel.id, ondelete='CASCADE'), nullable=False)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey(SourceModel.id, ondelete='CASCADE'), nullable=False)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(SheetModel.id, ondelete='CASCADE'), nullable=False,
                                          unique=True)
    interval_id: Mapped[int] = mapped_column(Integer, ForeignKey(IntervalModel.id, ondelete='RESTRICT'), nullable=False,
                                             unique=True)
    updated_at: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP(timezone=True), default=func.now(), onupdate=func.now())


    def to_entity(self, interval: Interval) -> Report:
        return Report(
            id=self.id,
            category=CategoryEnum(self.category_id).name,
            title=self.title,
            group_id=self.group_id,
            source_id=self.source_id,
            sheet_id=self.sheet_id,
            interval=interval.copy()
        )


class ReportRepoPostgres(BasePostgres, ReportRepo):
    model = ReportModel

    def __init__(self, session: AsyncSession):
        super().__init__(session)
        self.__sheet_repo = SheetRepoPostgres(session)
        self.__interval_repo = IntervalRepoPostgres(session)

    async def create_one(self, data: FullReportCreateSchema) -> Report:
        # Create sheet model
        sheet_data = SheetCreateSchema(
            df=data.sheet.dataframe,
            drop_index=data.sheet.drop_index,
            drop_columns=data.sheet.drop_columns,
            readonly_all_cells=data.sheet.readonly_all_cells,
        )
        sheet_id = await self.__sheet_repo.create_one(sheet_data)

        # Create interval model
        interval: IntervalModel = await self.__interval_repo.create_one(data.interval)

        # Create report model
        report_data = dict(
            title=data.title,
            category_id=CategoryEnum[data.category].value,
            source_id=data.source_id,
            group_id=data.group_id,
            interval_id=interval.id,
            sheet_id=sheet_id,
        )
        report_model: ReportModel = await super().create_one(report_data)
        return report_model.to_entity(interval.to_entity())

    async def get_one(self, filter_by: dict) -> Report:
        report_model: ReportModel = await super().get_one(filter_by)
        interval_model: IntervalModel = await self.__interval_repo.get_one({"id": report_model.interval_id})
        return report_model.to_entity(interval_model.to_entity())

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True, slice_from: int = None,
                       slice_to: int = None) -> list[Report]:
        report: pd.DataFrame = await self.get_many_as_frame(filter_by, order_by)
        interval: pd.DataFrame = await self.__interval_repo.get_many_as_frame({})

        report = pd.merge(
            report,
            interval.rename({'id': 'interval_id'}, axis=1),
            on='interval_id',
        )

        report_entities: list[Report] = []

        for i, row in report.iterrows():
            report_interval = Interval(
                id=row['interval_id'],
                total_start_date=row['total_start_date'],
                total_end_date=row['total_end_date'],
                start_date=row['start_date'],
                end_date=row['end_date'],
                period_year=row['period_year'],
                period_month=row['period_month'],
                period_day=row['period_day'],
            )
            report_entity = Report(
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

    async def update_one(self, data: DTO, filter_by: dict) -> Report:
        raise NotImplemented

    async def delete_one(self, filter_by: dict) -> Id_:
        deleted_model: ReportModel = await super().delete_one(filter_by)
        return deleted_model.id

    async def overwrite_linked_sheet(self, instance: Report, data: SheetCreateSchema) -> None:
        raise NotImplemented
