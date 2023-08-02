from collections import deque
from sqlalchemy.ext.asyncio import AsyncSession
from core_types import Event

from . import events
from . import handlers

HANDLERS = {
    events.SheetCreated: [handlers.handle_sheet_created],
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
