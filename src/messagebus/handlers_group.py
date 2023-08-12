import loguru
from src import finrep
from src.sheet import events as sheet_events

from src.group import entities as group_entities
from src.group import events as group_events

from .handler_service import HandlerService as HS


async def handle_group_created(hs: HS, event: group_events.GroupCreated):
    fn = finrep.FinrepFactory(event.category)
    event = event.copy()

    # Create group_df
    wire_df = await hs.wire_service.get_many_as_frame({"sheet_id": event.sheet_id})
    wire = fn.create_wire(wire_df)
    group_df = fn.create_group_from_wire(wire, ccols=event.columns, fixed_ccols=event.columns).get_group_df()

    # Create sheet
    event.sheet_id = await hs.sheet_service.create_one(
        sheet_events.SheetCreated(df=group_df, drop_index=True, drop_columns=False))

    # Create group from sheet_id and other group data
    group = await hs.group_service.create_one(event)
    hs.results[group_events.GroupCreated] = group


async def handle_group_gotten(hs: HS, event: group_events.GroupGotten):
    group: group_entities.Group = await hs.group_service.get_one({"id": event.group_id})
    hs.results[group_events.GroupGotten] = group

    if group.sheet.updated_at < group.source.updated_at:
        hs.queue.append(group_events.ParentUpdated(group_instance=group))


async def handle_group_list_gotten(hs: HS, _event: group_events.GroupListGotten):
    groups = await hs.group_service.get_many(filter_by={})
    hs.results[group_events.GroupListGotten] = groups


async def handle_group_partial_updated(hs: HS, event: group_events.GroupPartialUpdated):
    loguru.logger.warning(event)
    if event.id is None:
        raise ValueError
    data = event.dict()
    filter_by = {"id": data.pop('id')}
    updated = await hs.group_service.update_one(data, filter_by)
    hs.results[group_events.GroupPartialUpdated] = updated


async def handle_parent_updated(hs: HS, event: group_events.ParentUpdated):
    fn = finrep.FinrepFactory(event.group_instance.category.value)

    old_group_df = await hs.sheet_service.get_one_as_frame(
        sheet_events.SheetGotten(sheet_id=event.group_instance.sheet.id))

    #  Create new group df
    wire_df = await hs.wire_service.get_many_as_frame({"source_id": event.group_instance.source.id})
    wire = fn.create_wire(wire_df)
    new_group_df = (
        fn.create_group_from_frame(old_group_df, event.group_instance.ccols, event.group_instance.fixed_columns)
        .update_group(wire)
        .get_group_df()
    )

    # Update sheet with new group df
    await hs.sheet_service.overwrite_one(
        sheet_id=event.group_instance.sheet.id,
        data=sheet_events.SheetCreated(df=new_group_df, drop_index=True, drop_columns=False)
    )

    group_entity = group_entities.Group(**event.group_instance.dict())
    group_entity.sheet_df = new_group_df
    hs.results[group_events.ParentUpdated] = group_entity

    # Append next events
    hs.queue.append(sheet_events.SheetInfoUpdated(sheet_id=group_entity.sheet.id, data={}))


async def handle_group_sheet_updated(hs: HS, event: group_events.GroupSheetUpdated):
    updated = await hs.group_service.update_one({}, event.group_filter_by)
    hs.results[group_events.GroupSheetUpdated] = updated


async def handle_group_deleted(hs: HS, event: group_events.GroupDeleted):
    deleted_id = await hs.group_service.delete_one(filter_by={"id": event.group_id})
    hs.results[group_events.GroupDeleted] = deleted_id


HANDLERS_GROUP = {
    group_events.GroupCreated: [handle_group_created],
    group_events.GroupGotten: [handle_group_gotten],
    group_events.GroupListGotten: [handle_group_list_gotten],
    group_events.GroupPartialUpdated: [handle_group_partial_updated],
    group_events.ParentUpdated: [handle_parent_updated],
    group_events.GroupSheetUpdated: [handle_group_sheet_updated],
    group_events.GroupDeleted: [handle_group_deleted],
}
