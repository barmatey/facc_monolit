from collections import deque

import loguru
from sqlalchemy.ext.asyncio import AsyncSession

from src import core_types
from src.core_types import Event
from src.repository_postgres_new import SourceRepoPostgres, WireRepoPostgres

from . import events
from .entities import Wire
from .service import CrudService


async def handle_wire_created(event: events.WireCreated, session: AsyncSession, queue: deque) -> Wire:
    wire_repo = WireRepoPostgres(session)
    wire_service = CrudService(wire_repo)
    created: Wire = await wire_service.create_one(event)
    queue.append(events.SourceUpdated(source_id=created.source_id))
    return created


async def handle_wire_deleted(event: events.WireDeleted, session: AsyncSession, queue: deque) -> core_types.Id_:
    wire_repo = WireRepoPostgres(session)
    wire_service = CrudService(wire_repo)
    deleted = await wire_service.delete_one(filter_by={"id": event.wire_id})
    queue.append(events.SourceUpdated(source_id=deleted.source_id))
    return deleted.id


async def handle_source_updated(event: events.SourceUpdated, session: AsyncSession, _queue: deque) -> None:
    loguru.logger.debug('handle_source_updated')
    source_repo = SourceRepoPostgres(session)
    source_service = CrudService(source_repo)
    _ = await source_service.update_one({}, filter_by={"id": event.source_id})


HANDLERS = {
    events.WireCreated: [handle_wire_created],
    events.WireDeleted: [handle_wire_deleted],
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
