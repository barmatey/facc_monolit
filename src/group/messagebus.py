from collections import deque
from sqlalchemy.ext.asyncio import AsyncSession

from src.service_finrep import get_finrep
from src.core_types import Event
from src.repository_postgres_new import GroupRepoPostgres, WireRepoPostgres

from .entities import Group, ExpandedGroup
from .service import GroupService

from . import events


async def handle_group_gotten(event: events.GroupGotten, session: AsyncSession, queue: deque) -> Group:
    group_repo = GroupRepoPostgres(session)
    wire_repo = WireRepoPostgres(session)
    group_service = GroupService(group_repo, wire_repo, get_finrep())
    group = await group_service.get_one({"id": event.group_id})
    if group.updated_at > group.source.updated_ad:
        queue.append(events.SourceUpdated(group_instance=group))
    return group


async def handle_source_updated(event: events.SourceUpdated, session: AsyncSession, _queue: deque) -> ExpandedGroup:
    group_repo = GroupRepoPostgres(session)
    wire_repo = WireRepoPostgres(session)
    group_service = GroupService(group_repo, wire_repo, get_finrep())
    group: ExpandedGroup = await group_service.total_recalculate(event.group_instance)
    return group


HANDLERS = {
    events.GroupGotten: [handle_group_gotten],
    events.SourceUpdated: [handle_source_updated],
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
