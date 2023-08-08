from collections import deque
from sqlalchemy.ext.asyncio import AsyncSession

from src.wire.service import CrudService
from src.sheet.service import SheetService
from src.group.service import GroupService
from src.rep.service import ReportService

from src.repository_postgres_new import (GroupRepoPostgres, SourceRepoPostgres, WireRepoPostgres, SheetRepoPostgres,
                                         ReportRepoPostgres)


class HandlerService:
    def __init__(self, session: AsyncSession):
        self.queue = deque()
        self.results = {}
        self.wire_service = CrudService(WireRepoPostgres(session))
        self.source_service = CrudService(SourceRepoPostgres(session))
        self.sheet_service = SheetService(SheetRepoPostgres(session))
        self.group_service = GroupService(GroupRepoPostgres(session))
        self.report_service = ReportService(ReportRepoPostgres(session))
