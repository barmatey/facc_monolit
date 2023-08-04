from src.service_finrep import get_finrep

from src.sheet import events as sheet_events
from src.group import events as group_events
from src.rep import events as report_events

from src.rep import entities as report_entities

from .handler_service import HandlerService as HS


async def handle_report_created(hs: HS, event: report_events.ReportCreated) -> dict[str, report_entities.Report]:
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
    return {"core": report}


async def handle_report_gotten(hs: HS, event: report_events.ReportGotten) -> dict[str, report_entities.Report]:
    filter_by = {"id": event.report_id}
    report: report_entities.Report = await hs.report_service.get_one(filter_by)
    if report.group.updated_at < report.source.updated_at:
        hs.queue.append(group_events.ParentUpdated)
    if report.updated_at < report.group.updated_at or report.updated_at < report.source.updated_at:
        hs.queue.append(report_events.ParentUpdated)
    return {"core": report}


async def handle_parent_updated(hs: HS, event: report_events.ParentUpdated) -> dict[str, report_entities.Report]:
    # Create new report df
    group_df = await hs.sheet_service.get_one_as_frame(
        sheet_events.SheetGotten(sheet_id=event.report_instance.group.sheet_id))
    wire_df = await hs.wire_service.get_many_as_frame({"source_id": event.report_instance.source.id})
    interval = get_finrep(event.report_instance.category.value).create_interval(**event.report_instance.interval.dict())
    finrep = get_finrep(event.report_instance.category.value)
    new_report_df = finrep.create_report(wire_df, group_df, interval)

    # Update sheet with new report_df
    await hs.sheet_service.overwrite_one(
        sheet_id=event.report_instance.sheet.id,
        data=sheet_events.SheetCreated(df=new_report_df, drop_index=False, drop_columns=False)
    )

    # Change report updated_at field
    hs.queue.append(report_events.ReportSheetUpdated(report_instance=event.report_instance))

    result = report_entities.Report(**event.report_instance.dict())
    return {"core": result}


async def handle_report_list_gotten(hs, event: report_events.ReportListGotten) -> dict[str, list[report_entities.Report]]:
    filter_by = {"category_id": event.category.id}
    reports: list[report_entities.Report] = await hs.report_service.get_many(filter_by)
    return {"core": reports}


HANDLERS_REPORT = {
    report_events.ReportCreated: [handle_report_created],
    report_events.ReportGotten: [handle_report_gotten],
    report_events.ReportListGotten: [handle_report_list_gotten],
    report_events.ParentUpdated: NotImplemented,
    report_events.ReportSheetUpdated: NotImplemented,
}
