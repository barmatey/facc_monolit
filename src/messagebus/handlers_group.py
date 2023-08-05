import pandas as pd

from src import core_types
from src.service_finrep import get_finrep

from src.sheet import events as sheet_events

from src.group import entities as group_entities
from src.group import events as group_events

from .handler_service import HandlerService as HS


async def handle_group_created(hs: HS, event: group_events.GroupCreated):
    event = event.copy()

    # Create sheet
    wire_df = await hs.wire_service.get_many_as_frame({"sheet_id": event.sheet_id})
    finrep = get_finrep(event.category)
    group_df = finrep.create_group(wire_df, target_columns=event.columns)
    event.sheet_id = await hs.sheet_service.create_one(
        sheet_events.SheetCreated(df=group_df, drop_index=True, drop_columns=False))

    # Create group from sheet_id and other group data
    group = await hs.group_service.create_one(event)
    hs.results[group_events.GroupCreated] = group


async def handle_group_gotten(hs: HS, event: group_events.GroupGotten):
    group: group_entities.ExpandedGroup = await hs.group_service.get_one({"id": event.group_id})
    hs.results[group_events.GroupGotten] = group

    if group.updated_at < group.source.updated_at:
        hs.queue.append(group_events.ParentUpdated(group_instance=group))


async def handle_group_list_gotten(hs: HS, _event: group_events.GroupListGotten):
    groups = await hs.group_service.get_many(filter_by={})
    hs.results[group_events.GroupListGotten] = groups


async def handle_group_partial_updated(hs: HS, event: group_events.GroupListGotten):
    data = event.dict()
    filter_by = {"id": data.pop('id')}
    updated = await hs.group_service.update_one(data, filter_by)
    hs.results[group_events.GroupListGotten] = updated


async def handle_parent_updated(hs: HS, event: group_events.ParentUpdated):
    old_group_df = await hs.sheet_service.get_one_as_frame(
        sheet_events.SheetGotten(sheet_id=event.group_instance.sheet_id))

    # Create new group df
    wire_df = await hs.wire_service.get_many_as_frame({"source_id": event.group_instance.source_id})
    finrep = get_finrep(event.group_instance.category)
    new_group_df = finrep.create_group(wire_df, target_columns=event.group_instance.columns)
    if len(event.group_instance.fixed_columns):
        new_group_df = pd.merge(
            old_group_df[event.group_instance.fixed_columns],
            new_group_df,
            on=event.group_instance.fixed_columns, how='left'
        )

    # Update sheet with new group df
    await hs.sheet_service.overwrite_one(
        sheet_id=event.group_instance.sheet_id,
        data=sheet_events.SheetCreated(df=new_group_df, drop_index=True, drop_columns=False)
    )

    group = group_entities.ExpandedGroup(**event.group_instance.dict())
    group.sheet_df = new_group_df
    hs.results[group_events.ParentUpdated] = group

    # Append next events
    hs.queue.append(group_events.GroupSheetUpdated(group_filter_by={"id": event.group_instance.id}))


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
