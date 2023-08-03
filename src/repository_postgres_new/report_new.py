import loguru
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core_types import Id_, OrderBy, DTO
from src.rep.repository import ReportRepository
from src.rep import events, entities

from .base import BasePostgres
from .category import CategoryModel
from .group import GroupModel
from .report import ReportModel as ReportModelOld
from .source import SourceModel
from .sheet import SheetModel
from .interval import IntervalModel, IntervalRepoPostgres


class ReportModel(ReportModelOld):
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
        raise NotImplemented

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True, slice_from: int = None,
                       slice_to: int = None) -> list[entities.Report]:
        session = self._session
        filters = self._parse_filters(filter_by)
        stmt = (
            select(ReportModel, CategoryModel, SourceModel, GroupModel, SheetModel, IntervalModel)
            .join(CategoryModel, ReportModel.category_id == CategoryModel.id)
            .join(SourceModel, ReportModel.source_id == SourceModel.id)
            .join(GroupModel, ReportModel.group_id == GroupModel.id)
            .join(SheetModel, ReportModel.sheet_id == SourceModel.id)
            .join(IntervalModel, ReportModel.interval_id == IntervalModel.id)
            .where(*filters)
        )

        result = await session.execute(stmt)
        result = result.fetchall()
        # if len(result) == 0:
        #     raise LookupError(f"filter_by: {filter_by}")
        # result = result[0]
        # loguru.logger.debug(result)

        return []

    async def update_one(self, data: DTO, filter_by: dict) -> entities.Report:
        raise NotImplemented

    async def delete_one(self, filter_by: dict) -> Id_:
        raise NotImplemented
