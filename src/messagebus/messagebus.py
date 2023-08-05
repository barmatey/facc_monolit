from typing import Dict, Literal, Any

import loguru
from sqlalchemy.ext.asyncio import AsyncSession
from src.core_types import Event

from .handler_service import HandlerService
from .handlers_report import HANDLERS_REPORT
from .handlers_group import HANDLERS_GROUP
from .handlers_sheet import HANDLERS_SHEET


HANDLERS = (
        HANDLERS_GROUP
        | HANDLERS_REPORT
        | HANDLERS_SHEET
)


async def handle(event: Event, session: AsyncSession):
    hs = HandlerService(session)
    hs.queue.append(event)
    while hs.queue:
        event = hs.queue.popleft()
        for handler in HANDLERS[type(event)]:
            # Important! handler function changes HandlerService queue state and results state
            await handler(hs, event)
    return hs.results
