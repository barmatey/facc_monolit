from sqlalchemy import select, String, Integer, ForeignKey, TIMESTAMP, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from src.core_types import Id_, OrderBy, DTO
from src.rep.repository import ReportRepository
from src.rep import events, entities

from .base import BasePostgres, BaseModel
from .category import CategoryModel
from .group import GroupModel
from .source import SourceModel
from .sheet import SheetModel
from .interval import IntervalModel, IntervalRepoPostgres


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

    def to_entity(self,
                  interval: entities.Interval,
                  category: entities.InnerCategory,
                  group: entities.InnerGroup,
                  source: entities.InnerSource,
                  sheet: entities.InnerSheet,
                  ) -> entities.Report:
        return entities.Report(
            id=self.id,
            title=self.title,
            category=category,
            group=group,
            source=source,
            interval=interval,
            sheet=sheet,
            updated_at=self.updated_at,
        )


class ReportRepoPostgres(BasePostgres, ReportRepository):
    model = ReportModel

    def __init__(self, session: AsyncSession):
        super().__init__(session)
        self.__interval_repo = IntervalRepoPostgres(session)

    async def create_one(self, event: events.ReportCreated) -> entities.Report:
        interval_model: IntervalModel = await self.__interval_repo.create_one(event.interval)
        report_model = self.model(
            title=event.title,
            category_id=event.category.id,
            group_id=event.group.id,
            source_id=event.source.id,
            sheet_id=event.sheet.id,
            interval_id=interval_model.id,
        )
        self._session.add(report_model)
        await self._session.flush()
        return report_model.to_entity(interval=interval_model.to_entity(), category=event.category, group=event.group,
                                      source=event.source, sheet=event.sheet)

    async def get_one(self, filter_by: dict) -> entities.Report:
        reports = await self.get_many(filter_by)
        if len(reports) != 1:
            raise LookupError(f"filter_by: {filter_by}")
        return reports[0]

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True, slice_from: int = None,
                       slice_to: int = None) -> list[entities.Report]:
        session = self._session
        filters = self._parse_filters(filter_by)
        stmt = (
            select(ReportModel, CategoryModel, SourceModel, GroupModel, SheetModel, IntervalModel)
            .join(CategoryModel, ReportModel.category_id == CategoryModel.id)
            .join(SourceModel, ReportModel.source_id == SourceModel.id)
            .join(GroupModel, ReportModel.group_id == GroupModel.id)
            .join(SheetModel, ReportModel.sheet_id == SheetModel.id)
            .join(IntervalModel, ReportModel.interval_id == IntervalModel.id)
            .where(*filters)
        )

        result = await session.execute(stmt)
        result = result.fetchall()

        result = [
            x[0].to_entity(
                category=x[1].to_entity(),
                source=entities.InnerSource(id=x[2].id, title=x[2].title, updated_at=x[2].updated_at),
                group=entities.InnerGroup(id=x[3].id, title=x[3].title, updated_at=x[3].updated_at,
                                          sheet_id=x[3].sheet_id, ccols=x[3].columns, fixed_ccols=x[3].fixed_columns),
                sheet=entities.InnerSheet(id=x[4].id, updated_at=x[4].updated_at),
                interval=x[5].to_entity(),
            )
            for x in result
        ]
        return result

    async def update_one(self, data: DTO, filter_by: dict) -> entities.Report:
        _ = await super().update_one(data, filter_by)
        return await self.get_one(filter_by)

    async def delete_one(self, filter_by: dict) -> Id_:
        deleted_model = await super().delete_one(filter_by)
        return deleted_model.id
