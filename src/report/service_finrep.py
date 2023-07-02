from abc import ABC, abstractmethod
from . import entities, schema


class FinrepService(ABC):

    @abstractmethod
    async def create_group(self, data: schema.GroupCreateSchema) -> entities.Group:
        pass

    @abstractmethod
    async def create_report(self, data: schema.ReportCreateSchema) -> entities.Report:
        pass
