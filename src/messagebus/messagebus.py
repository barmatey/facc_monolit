from typing import Dict, Literal, Any

import loguru
from sqlalchemy.ext.asyncio import AsyncSession
from src.core_types import Event

from .handler_service import HandlerService
from .handlers_report import HANDLERS_REPORT
from .handlers_group import HANDLERS_GROUP
from .handlers_sheet import HANDLERS_SHEET

Result = Dict[Literal['core', 'no_matter'], Any]

HANDLERS = (
        HANDLERS_GROUP
        | HANDLERS_REPORT
        | HANDLERS_SHEET
)


async def handle(event: Event, session: AsyncSession):
    hs = HandlerService(session)
    hs.queue.append(event)
    result: Result = {}
    while hs.queue:
        event = hs.queue.popleft()
        for handler in HANDLERS[type(event)]:
            # Important! handler function changes HandlerService queue state
            result = result | await handler(hs, event)
    return result["core"]
