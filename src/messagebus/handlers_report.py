from src import core_types
from src.service_finrep import get_finrep

from src.sheet import events as sheet_events
from src.group import events as group_events
from src.rep import events as report_events

from src.rep import entities as report_entities
from src.group import entities as group_entities

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
        sheet_events.SheetCreated(df=report_df, drop_index=False, drop_columns=False, readonly_all_cells=True))
    event.sheet = report_entities.InnerSheet(id=sheet_id)

    # Create report
    report: report_entities.Report = await hs.report_service.create_one(event)
    return {"core": report}


async def handle_report_gotten(hs: HS, event: report_events.ReportGotten) -> dict[str, report_entities.Report]:
    filter_by = {"id": event.report_id}
    report: report_entities.Report = await hs.report_service.get_one(filter_by)

    if report.group.updated_at < report.source.updated_at:
        group: group_entities.Group = await hs.group_service.get_one(filter_by={"id": report.group.id})
        hs.queue.append(group_events.ParentUpdated(group_instance=group))

    if report.updated_at < report.group.updated_at or report.updated_at < report.source.updated_at:
        hs.queue.append(report_events.ParentUpdated(report_instance=report))

    return {"core": report}


async def handle_parent_updated(hs: HS, event: report_events.ParentUpdated) -> dict[str, report_entities.Report]:
    # Create new report df
    group_df = await hs.sheet_service.get_one_as_frame(
        sheet_events.SheetGotten(sheet_id=event.report_instance.group.sheet_id))
    wire_df = await hs.wire_service.get_many_as_frame({"source_id": event.report_instance.source.id})
    interval = event.report_instance.interval.dict()
    interval.pop("id")
    interval = get_finrep(event.report_instance.category.value).create_interval(**interval)
    finrep = get_finrep(event.report_instance.category.value)
    new_report_df = finrep.create_report(wire_df, group_df, interval)

    # Update sheet with new report_df
    await hs.sheet_service.overwrite_one(
        sheet_id=event.report_instance.sheet.id,
        data=sheet_events.SheetCreated(df=new_report_df, drop_index=False, drop_columns=False, readonly_all_cells=True)
    )

    # Change report updated_at field
    hs.queue.append(report_events.ReportSheetUpdated(report_instance=event.report_instance))

    result = report_entities.Report(**event.report_instance.dict())
    return {"core": result}


async def handle_report_list_gotten(hs, event: report_events.ReportListGotten) -> dict[str, list]:
    filter_by = {"category_id": event.category.id}
    reports: list[report_entities.Report] = await hs.report_service.get_many(filter_by)
    return {"core": reports}


async def handle_report_sheet_updated(hs: HS, event: report_events.ReportSheetUpdated):
    _ = await hs.report_service.update_one({}, filter_by={"id": event.report_instance.id})
    return {"no_matter": None}


async def handle_report_deleted(hs: HS, event: report_events.ReportDeleted) -> dict[str, core_types.Id_]:
    deleted_id = await hs.report_service.delete_one(filter_by={"id": event.report_id})
    return {"core": deleted_id}


HANDLERS_REPORT = {
    report_events.ReportCreated: [handle_report_created],
    report_events.ReportGotten: [handle_report_gotten],
    report_events.ReportListGotten: [handle_report_list_gotten],
    report_events.ParentUpdated: [handle_parent_updated],
    report_events.ReportSheetUpdated: [handle_report_sheet_updated],
    report_events.ReportDeleted: [handle_report_deleted],
}
