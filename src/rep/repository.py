from abc import ABC, abstractmethod

from src.core_types import DTO, OrderBy, Id_
from .entities import Report, Interval


class ReportRepository(ABC):
    @abstractmethod
    async def create_one(self, data: DTO) -> Report:
        raise NotImplemented

    @abstractmethod
    async def get_one(self, filter_by: dict) -> Report:
        raise NotImplemented

    @abstractmethod
    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                       slice_from: int = None, slice_to: int = None):
        raise NotImplemented

    @abstractmethod
    async def update_one(self, data: DTO, filter_by: dict) -> Report:
        raise NotImplemented

    @abstractmethod
    async def delete_one(self, filter_by: dict) -> Id_:
        raise NotImplemented
