from src.service_finrep import get_finrep

from src.sheet import events as sheet_events
from src.group import events as group_events
from src.rep import events as report_events

from src.rep import entities as report_entities

from .handler_service import HandlerService


async def handle_report_created(hs: HandlerService, event: report_events.ReportCreated) -> report_entities.Report:
    event = event.copy()
    finrep = get_finrep(event.category.value)

    # Get data
    wire_df = await hs.wire_service.get_many_as_frame({"source_id": event.source.id})
    group_df = await hs.group_service.get_linked_frame(group_id=event.group.id)
    report_df = finrep.create_report(wire_df, group_df, finrep.create_interval(**event.interval.dict()))

    # Create sheet
    sheet_id = await hs.sheet_service.create_one(
        sheet_events.SheetCreated(df=report_df, drop_index=False, drop_columns=False))
    event.sheet = report_entities.InnerSheet(id=sheet_id)

    # Create report
    report: report_entities.Report = await hs.report_service.create_one(event)
    return report


async def handle_report_gotten(hs: HandlerService, event: report_events.ReportGotten) -> report_entities.Report:
    filter_by = {"id": event.report_id}
    report: report_entities.Report = await hs.report_service.get_one(filter_by)
    if report.group.updated_at < report.source.updated_at:
        hs.queue.append(group_events.GroupParentUpdated)
    if report.updated_at < report.group.updated_at or report.updated_at < report.source.updated_at:
        hs.queue.append(report_events.ReportParentUpdated)
    return report


async def handle_report_list_gotten(hs, event: report_events.ReportListGotten) -> list[report_entities.Report]:
    filter_by = {"category_id": event.category.id}
    reports: list[report_entities.Report] = await hs.report_service.get_many(filter_by)
    return reports


HANDLERS_REPORT = {
    report_events.ReportCreated: [handle_report_created],
    report_events.ReportGotten: [handle_report_gotten],
    report_events.ReportListGotten: [handle_report_list_gotten],
    report_events.ReportParentUpdated: NotImplemented,
}
