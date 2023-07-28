from sqlalchemy import String, Integer, ForeignKey
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
    model = Report

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
        raise NotImplemented

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True, slice_from: int = None,
                       slice_to: int = None) -> list[Report]:
        raise NotImplemented

    async def update_one(self, data: DTO, filter_by: dict) -> Report:
        raise NotImplemented

    async def delete_one(self, filter_by: dict) -> Id_:
        raise NotImplemented

    async def overwrite_linked_sheet(self, instance: Report, data: SheetCreateSchema) -> None:
        raise NotImplemented
