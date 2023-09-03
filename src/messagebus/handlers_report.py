import loguru

from src import finrep

from src.sheet import events as sheet_events
from src.group import events as group_events
from src.rep import events as report_events

from src.rep import entities as report_entities
from src.group import entities as group_entities

from .handler_service import HandlerService as HS


async def handle_report_created(hs: HS, event: report_events.ReportCreated):
    event = event.model_copy()
    frep = finrep.FinrepFactory(event.category.value)

    # Create report_df
    wire_df = await hs.wire_service.get_many_as_frame({"source_id": event.source.id})
    wire = frep.create_wire(wire_df)

    group_df = await hs.group_service.get_linked_frame(group_id=event.group.id)
    group = frep.create_group_from_frame(group_df, ccols=event.group.ccols, fixed_ccols=event.group.fixed_ccols)

    interval = frep.create_interval(**event.interval.dict())

    report_df = (
        frep.create_report(wire, group, interval)
        .create_report_df()
        .sort_by_group()
        .drop_zero_rows()
        .calculate_total()
        .get_report_df()
    )

    # Create sheet
    sheet_id = await hs.sheet_service.create_one(
        sheet_events.SheetCreated(df=report_df, drop_index=False, drop_columns=False, readonly_all_cells=True))
    event.sheet = report_entities.InnerSheet(id=sheet_id)

    # Create report
    report: report_entities.Report = await hs.report_service.create_one(event)
    hs.results[report_events.ReportCreated] = report


async def handle_report_gotten(hs: HS, event: report_events.ReportGotten):
    # todo maybe I have to join the next two lines in one request
    report: report_entities.Report = await hs.report_service.get_one(filter_by={"id": event.report_id})
    group: group_entities.Group = await hs.group_service.get_one(filter_by={"id": report.group.id})
    hs.results[report_events.ReportGotten] = report

    if group.sheet.updated_at < group.source.updated_at:
        hs.queue.append(group_events.ParentUpdated(group_instance=group))

    if report.sheet.updated_at < group.sheet.updated_at or report.sheet.updated_at < report.source.updated_at:
        hs.queue.append(report_events.ParentUpdated(report_instance=report))


async def handle_parent_updated(hs: HS, event: report_events.ParentUpdated):
    # Create new report df
    frep = finrep.FinrepFactory(event.report_instance.category.value)

    wire_df = await hs.wire_service.get_many_as_frame({"source_id": event.report_instance.source.id})
    wire = frep.create_wire(wire_df)

    group_sheet_id = event.report_instance.group.sheet_id
    group_df = await hs.sheet_service.get_one_as_frame(sheet_events.SheetGotten(sheet_id=group_sheet_id))
    group = frep.create_group_from_frame(group_df, ccols=event.report_instance.group.ccols)

    interval = event.report_instance.interval.model_dump()
    interval.pop("id")
    interval = frep.create_interval(**interval)

    new_report_df = (
        frep.create_report(wire, group, interval)
        .create_report_df()
        .sort_by_group()
        .calculate_total()
        .drop_zero_rows()
        .get_report_df()
    )

    # Update sheet with new report_df
    await hs.sheet_service.overwrite_one(
        sheet_id=event.report_instance.sheet.id,
        data=sheet_events.SheetCreated(df=new_report_df, drop_index=False, drop_columns=False, readonly_all_cells=True)
    )
    hs.results[report_events.ParentUpdated] = report_entities.Report(**event.report_instance.dict())

    # Change sheet updated_at field
    hs.queue.append(sheet_events.SheetInfoUpdated(sheet_id=event.report_instance.sheet.id, data={}))


async def handle_report_list_gotten(hs: HS, event: report_events.ReportListGotten):
    filter_by = {"category_id": event.category.id}
    reports: list[report_entities.Report] = await hs.report_service.get_many(filter_by)
    hs.results[report_events.ReportListGotten] = reports


async def handle_report_sheet_updated(hs: HS, event: report_events.ReportSheetUpdated):
    report = await hs.report_service.update_one({}, filter_by={"id": event.report_instance.id})
    hs.results[report_events.ReportSheetUpdated] = report


async def handle_report_deleted(hs: HS, event: report_events.ReportDeleted):
    deleted_id = await hs.report_service.delete_one(filter_by={"id": event.report_id})
    hs.results[report_events.ReportDeleted] = deleted_id


async def handle_report_checker_created(hs: HS, event: report_events.ReportCheckerCreated):
    report_df = await hs.sheet_service.sheet_repo.get_one_as_frame(sheet_id=event.report_instance.sheet.id)
    checker_df = report_df
    for col in checker_df.columns:
        checker_df[col] = 0

    sheet_created = sheet_events.SheetCreated(df=checker_df, drop_index=False, drop_columns=False)
    sheet_id = await hs.sheet_service.create_one(data=sheet_created)
    print(report_df)
    stop


HANDLERS_REPORT = {
    report_events.ReportCreated: [handle_report_created],
    report_events.ReportGotten: [handle_report_gotten],
    report_events.ReportListGotten: [handle_report_list_gotten],
    report_events.ParentUpdated: [handle_parent_updated],
    report_events.ReportSheetUpdated: [handle_report_sheet_updated],
    report_events.ReportDeleted: [handle_report_deleted],
    report_events.ReportCheckerCreated: [handle_report_checker_created],
}
