from collections import deque

from sqlalchemy.ext.asyncio import AsyncSession

from src import core_types
from src.repository_postgres_new import SheetRepoPostgres
from . import events
from .service import SheetService


async def handle_sheet_created(event: events.SheetCreated, session: AsyncSession, queue: deque) -> core_types.Id_:
    sheet_repo = SheetRepoPostgres(session)
    sheet_service = SheetService(sheet_repo)
    sheet_id: core_types.Id_ = await sheet_service.create_one(event)
    return sheet_id
