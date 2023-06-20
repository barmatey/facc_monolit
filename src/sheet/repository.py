from abc import ABC, abstractmethod

from . import entities
import repository_postgres


class Repository(ABC):
    @abstractmethod
    async def retrieve_sheet(self, data: entities.SheetRetrieve) -> entities.Sheet:
        pass


class PostgresRepo(Repository):
    repo = repository_postgres.SheetRepo

    async def retrieve_sheet(self, data: entities.SheetRetrieve) -> entities.Sheet:
        sheet = await self.repo().retrieve_as_sheet(id_=data.sheet_id)
        return sheet
