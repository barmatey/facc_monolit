import pandas as pd

from src import core_types
from src.service_finrep import get_finrep

from src.sheet import events as sheet_events

from src.group import entities as group_entities
from src.group import events as group_events

from .handler_service import HandlerService


async def handle_group_created(hs: HandlerService, event: group_events.GroupCreated) -> group_entities.Group:
    event = event.copy()

    # Create sheet
    wire_df = await hs.wire_service.get_many_as_frame({"sheet_id": event.sheet_id})
    finrep = get_finrep(event.category)
    group_df = finrep.create_group(wire_df, target_columns=event.columns)
    event.sheet_id = await hs.sheet_service.create_one(
        sheet_events.SheetCreated(df=group_df, drop_index=True, drop_columns=False))

    # Create group from sheet_id and other group data
    group = await hs.group_service.create_one(event)
    return group


async def handle_group_gotten(hs: HandlerService, event: group_events.GroupGotten) -> group_entities.Group:
    group = await hs.group_service.get_one({"id": event.group_id})
    if group.updated_at < group.source.updated_ad:
        hs.queue.append(group_events.GroupParentUpdated(group_instance=group))
    return group


async def handle_group_list_gotten(hs: HandlerService, _event: group_events.GroupListGotten) -> list[
    group_entities.Group]:
    groups = await hs.group_service.get_many(filter_by={})
    return groups


async def handle_group_partial_updated(hs: HandlerService, event: group_events.GroupListGotten) -> group_entities.Group:
    data = event.dict()
    filter_by = {"id": data.pop('id')}
    updated = await hs.group_service.update_one(data, filter_by)
    return updated


async def handle_parent_updated(hs: HandlerService,
                                event: group_events.GroupParentUpdated) -> group_entities.ExpandedGroup:
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

    hs.queue.append(group_events.GroupSheetUpdated(group_id=event.group_instance.id))

    result = group_entities.ExpandedGroup(**event.group_instance.dict())
    result.sheet_df = new_group_df
    return result


async def handle_group_sheet_updated(hs: HandlerService, event: group_events.GroupSheetUpdated):
    _ = await hs.group_service.update_one({}, filter_by={"id": event.group_id})


async def handle_group_deleted(hs: HandlerService, event: group_events.GroupDeleted) -> core_types.Id_:
    deleted_id = await hs.group_service.delete_one(filter_by={"id": event.group_id})
    return deleted_id


HANDLERS_GROUP = {
    group_events.GroupCreated: [handle_group_created],
    group_events.GroupGotten: [handle_group_gotten],
    group_events.GroupListGotten: [handle_group_list_gotten],
    group_events.GroupPartialUpdated: [handle_group_partial_updated],
    group_events.GroupParentUpdated: [handle_parent_updated],
    group_events.GroupSheetUpdated: [handle_group_sheet_updated],
    group_events.GroupDeleted: [handle_group_deleted],
}
