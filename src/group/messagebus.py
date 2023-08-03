from collections import deque
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

import core_types
from src.core_types import Event
from src.service_finrep import get_finrep
from src.repository_postgres_new import GroupRepoPostgres, WireRepoPostgres, SheetRepoPostgres
from src.sheet.service import SheetService
from src.wire.service import CrudService
from src.sheet import events as sheet_events

from .entities import Group, ExpandedGroup
from .service import GroupService
from . import events as group_events


async def handle_group_created(event: group_events.GroupCreated, session: AsyncSession, queue: deque) -> Group:
    event = event.copy()

    # Load wires
    wire_repo = WireRepoPostgres(session)
    wire_service = CrudService(wire_repo)
    wire_df = await wire_service.get_many_as_frame({"sheet_id": event.sheet_id})

    # Create group_df
    finrep = get_finrep(event.category)
    group_df = finrep.create_group(wire_df, target_columns=event.columns)

    # Create sheet from group_df
    sheet_repo = SheetRepoPostgres(session)
    sheet_service = SheetService(sheet_repo)
    event.sheet_id = await sheet_service.create_one(
        sheet_events.SheetCreated(df=group_df, drop_index=True, drop_columns=False))

    # Create group from sheet_id and other group data
    group_repo = GroupRepoPostgres(session)
    group_service = GroupService(group_repo)
    group = await group_service.create_one(event)
    return group


async def handle_group_gotten(event: group_events.GroupGotten, session: AsyncSession, queue: deque) -> Group:
    group_repo = GroupRepoPostgres(session)
    group_service = GroupService(group_repo)
    group = await group_service.get_one({"id": event.group_id})
    if group.updated_at < group.source.updated_ad:
        queue.append(group_events.ParentSourceUpdated(group_instance=group))
    return group


async def handle_group_list_gotten(_event: group_events.GroupListGotten, session: AsyncSession,
                                   _queue: deque) -> list[Group]:
    group_repo = GroupRepoPostgres(session)
    group_service = GroupService(group_repo)
    groups = await group_service.get_many(filter_by={})
    return groups


async def handle_group_partial_updated(event: group_events.GroupPartialUpdated, session: AsyncSession,
                                       _queue: deque) -> Group:
    group_repo = GroupRepoPostgres(session)
    group_service = GroupService(group_repo)
    data = event.dict()
    filter_by = {"id": data.pop('id')}
    updated = await group_service.update_one(data, filter_by)
    return updated


async def handle_source_updated(event: group_events.ParentSourceUpdated, session: AsyncSession,
                                queue: deque) -> ExpandedGroup:
    # Load wires
    wire_repo = WireRepoPostgres(session)
    wire_service = CrudService(wire_repo)
    wire_df = await wire_service.get_many_as_frame({"source_id": event.group_instance.source_id})

    # Get old group df
    sheet_repo = SheetRepoPostgres(session)
    sheet_service = SheetService(sheet_repo)
    old_group_df = await sheet_service.get_one_as_frame(
        sheet_events.SheetGotten(sheet_id=event.group_instance.sheet_id))

    # Create new group df
    finrep = get_finrep(event.group_instance.category)
    new_group_df = finrep.create_group(wire_df, target_columns=event.group_instance.columns)
    if len(event.group_instance.fixed_columns):
        new_group_df = pd.merge(
            old_group_df[event.group_instance.fixed_columns],
            new_group_df,
            on=event.group_instance.fixed_columns, how='left'
        )

    # Update sheet with new group df
    sheet_repo = SheetRepoPostgres(session)
    sheet_service = SheetService(sheet_repo)
    await sheet_service.overwrite_one(
        sheet_id=event.group_instance.sheet_id,
        data=sheet_events.SheetCreated(df=new_group_df, drop_index=True, drop_columns=False)
    )

    queue.append(group_events.GroupSheetUpdated(group_id=event.group_instance.id))

    result = ExpandedGroup(**event.group_instance.dict())
    result.sheet_df = new_group_df
    return result


async def handle_group_sheet_updated(event: group_events.GroupSheetUpdated, session: AsyncSession, _queue: deque):
    group_repo = GroupRepoPostgres(session)
    group_service = GroupService(group_repo)
    _ = await group_service.update_one({}, filter_by={"id": event.group_id})


async def handle_group_deleted(event: group_events.GroupDeleted, session: AsyncSession,
                               _queue: deque) -> core_types.Id_:
    group_repo = GroupRepoPostgres(session)
    group_service = GroupService(group_repo)
    deleted_id = await group_service.delete_one(filter_by={"id": event.group_id})
    return deleted_id


HANDLERS = {
    group_events.GroupCreated: [handle_group_created],
    group_events.GroupGotten: [handle_group_gotten],
    group_events.GroupListGotten: [handle_group_list_gotten],
    group_events.GroupPartialUpdated: [handle_group_partial_updated],
    group_events.ParentSourceUpdated: [handle_source_updated],
    group_events.GroupSheetUpdated: [handle_group_sheet_updated],
    group_events.GroupDeleted: [handle_group_deleted],
}


async def handle(event: Event, session: AsyncSession) -> list:
    queue = deque()
    queue.append(event)
    results = []
    while queue:
        event = queue.popleft()
        for handler in HANDLERS[type(event)]:
            # Important: handler changes the queue
            results.append(await handler(event, session, queue))
    return results
