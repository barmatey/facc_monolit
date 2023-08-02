from collections import deque

import loguru
from sqlalchemy.ext.asyncio import AsyncSession

from src.service_finrep import get_finrep
from src.core_types import Event
from src.repository_postgres_new import GroupRepoPostgres, WireRepoPostgres, SheetRepoPostgres
from src.sheet.service import SheetService
from src.wire.service import CrudService

from .entities import Group, ExpandedGroup
from .service import GroupService

from src.sheet import events as sheet_events
from src.sheet import handlers as sheet_handlers
from . import events as group_events


async def handle_group_created(event: group_events.GroupCreated, session: AsyncSession, queue: deque) -> Group:
    event = event.copy()

    # Create group df
    wire_repo = WireRepoPostgres(session)
    wire_service = CrudService(wire_repo)
    wire_df = await wire_service.get_many_as_frame({"sheet_id": event.sheet_id})
    finrep = get_finrep(event.category)
    group_df = finrep.create_group(wire_df, target_columns=event.columns)

    # Create sheet
    sheet_repo = SheetRepoPostgres(session)
    sheet_service = SheetService(sheet_repo)
    event.sheet_id = await sheet_service.create_one(
        sheet_events.SheetCreated(df=group_df, drop_index=True, drop_columns=False))

    # Create group
    group_repo = GroupRepoPostgres(session)
    group_service = GroupService(group_repo, wire_repo, get_finrep())
    group = await group_service.create_one(event)
    return group


async def handle_group_gotten(event: group_events.GroupGotten, session: AsyncSession, queue: deque) -> Group:
    group_repo = GroupRepoPostgres(session)
    wire_repo = WireRepoPostgres(session)
    group_service = GroupService(group_repo, wire_repo, get_finrep())
    group = await group_service.get_one({"id": event.group_id})
    if group.updated_at < group.source.updated_ad:
        queue.append(group_events.SourceUpdated(group_instance=group))
    return group


async def handle_source_updated(event: group_events.SourceUpdated, session: AsyncSession,
                                _queue: deque) -> ExpandedGroup:
    group_repo = GroupRepoPostgres(session)
    wire_repo = WireRepoPostgres(session)
    group_service = GroupService(group_repo, wire_repo, get_finrep())
    group: ExpandedGroup = await group_service.total_recalculate(event.group_instance)
    return group


HANDLERS = {
    group_events.GroupCreated: [handle_group_created],
    group_events.GroupGotten: [handle_group_gotten],
    group_events.SourceUpdated: [handle_source_updated],
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
