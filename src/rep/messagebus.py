from collections import deque

import loguru
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from src.core_types import Event

from src.wire.service import CrudService
from src.sheet.service import SheetService
from src.group.service import GroupService
from src.rep.service import ReportService

from src.sheet import events as sheet_events
from src.group import events as group_events
from src.rep import events as report_events
from src.rep import entities as report_entities

from src.repository_postgres_new import (GroupRepoPostgres, WireRepoPostgres, SheetRepoPostgres, ReportRepoPostgres)
from src.service_finrep import get_finrep


class MessageBus:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.queue = deque()
        self.wire_service = CrudService(WireRepoPostgres(session))
        self.sheet_service = SheetService(SheetRepoPostgres(session))
        self.group_service = GroupService(GroupRepoPostgres(session))
        self.report_service = ReportService(ReportRepoPostgres(session))
        self._HANDLERS = self._register_handlers()

    async def handle_report_created(self, event: report_events.ReportCreated) -> report_entities.Report:
        event = event.copy()
        finrep = get_finrep(event.category.value)

        # Get data
        wire_df = await self.wire_service.get_many_as_frame({"source_id": event.source.id})
        group_df = await self.group_service.get_linked_frame(group_id=event.group.id)
        report_df = finrep.create_report(wire_df, group_df, finrep.create_interval(**event.interval.dict()))

        # Create sheet
        sheet_id = await self.sheet_service.create_one(
            sheet_events.SheetCreated(df=report_df, drop_index=False, drop_columns=False))
        event.sheet = report_entities.InnerSheet(id=sheet_id)

        # Create report
        report: report_entities.Report = await self.report_service.create_one(event)
        return report

    async def handle_report_gotten(self, event: report_events.ReportGotten) -> report_entities.Report:
        filter_by = {"id": event.report_id}
        report: report_entities.Report = await self.report_service.get_one(filter_by)
        if report.group.updated_at < report.source.updated_at:
            self.queue.append(group_events.GroupParentUpdated)
        if report.updated_at < report.group.updated_at or report.updated_at < report.source.updated_at:
            self.queue.append(report_events.ReportParentUpdated)
        return report

    async def handle_report_list_gotten(self, event: report_events.ReportListGotten) -> list[report_entities.Report]:
        filter_by = {"category_id": event.category.id}
        reports: list[report_entities.Report] = await self.report_service.get_many(filter_by)
        return reports

    def _register_handlers(self):
        return {
            report_events.ReportCreated: [self.handle_report_created],
            report_events.ReportGotten: [self.handle_report_gotten],
            report_events.ReportListGotten: [self.handle_report_list_gotten],
            report_events.ReportParentUpdated: NotImplemented,
            group_events.GroupParentUpdated: NotImplemented,
        }

    async def handle(self, event: Event) -> list:
        self.queue.append(event)
        results = []
        while self.queue:
            event = self.queue.popleft()
            for handler in self._HANDLERS[type(event)]:
                results.append(await handler(event))
        return results

