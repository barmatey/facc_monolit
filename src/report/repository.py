import typing
from abc import ABC, abstractmethod

import pandas as pd

from src.core_types import OrderBy, Id_, DTO
from . import entities, schema

Entity = typing.TypeVar(
    'Entity',
    entities.Group,
    entities.Report,
    entities.ReportCategory,
)


class CrudRepo(ABC):
    @abstractmethod
    async def create_one(self, data: DTO) -> Entity:
        raise NotImplemented

    @abstractmethod
    async def get_one(self, filter_by: dict) -> Entity:
        raise NotImplemented

    @abstractmethod
    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                       slice_from: int = None, slice_to: int = None):
        raise NotImplemented

    @abstractmethod
    async def update_one(self, data: DTO, filter_by: dict) -> Entity:
        raise NotImplemented

    @abstractmethod
    async def delete_one(self, filter_by: dict) -> Id_:
        raise NotImplemented


class GroupRepo(CrudRepo, ABC):

    @abstractmethod
    async def get_linked_dataframe(self, group_id: Id_) -> pd.DataFrame:
        raise NotImplemented


class ReportRepo(CrudRepo, ABC):
    @abstractmethod
    async def overwrite_linked_sheet(self, instance: entities.Report, data: entities.SheetCreate) -> None:
        raise NotImplemented


class WireRepo(CrudRepo, ABC):

    @abstractmethod
    async def get_wire_dataframe(self, filter_by: dict, order_by: OrderBy = None) -> pd.DataFrame:
        raise NotImplemented
