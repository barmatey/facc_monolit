from sqlalchemy.ext.asyncio import AsyncSession
from src.core_types import Event

from .handler_service import HandlerService
from .handlers_report import HANDLERS_REPORT
from .handlers_group import HANDLERS_GROUP

HANDLERS = HANDLERS_GROUP | HANDLERS_REPORT


async def handle(event: Event, session: AsyncSession):
    hs = HandlerService(session)
    hs.queue.append(event)
    results = {}
    while hs.queue:
        event = hs.queue.popleft()
        for handler in HANDLERS[type(event)]:
            # Important! handler function changes HandlerService queue state
            results = results | await handler(hs, event)
    return results["core"]
