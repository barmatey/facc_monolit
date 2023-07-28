from src.report.entities import Group, ExpandedGroup, Report
from src import core_types
from src.report import service_crud as report_service
from src.wire import service as wire_service


async def total_recalculate_entities_linked_with_source(source_id: core_types.Id_) -> None:
    w_service = wire_service.ServiceWire()
    g_service = report_service.GroupService()
    r_service = report_service.ReportService()

    wire_df = await w_service.retrieve_bulk_as_dataframe({"source_id": source_id})

    linked_groups: list[Group] = await g_service.get_many({"source_id": source_id})
    updated_groups = [await g_service.total_recalculate(group, wire_df) for group in linked_groups]

    def get_group_by_id(id_: core_types.Id_) -> ExpandedGroup:
        for group in updated_groups:
            if group.id == id_:
                return group
        raise LookupError

    linked_reports: list[Report] = await r_service.get_many({"source_id": source_id})
    for report in linked_reports:
        group = get_group_by_id(report.group_id)
        _ = await r_service.total_recalculate(report, wire_df, group.sheet_df)
