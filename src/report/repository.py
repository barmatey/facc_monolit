import typing
from abc import ABC, abstractmethod

import pandas as pd

from core_types import OrderBy
from src import core_types
from src import repository_postgres as postgres
from . import entities, schema

Entity = typing.TypeVar(
    'Entity',
    entities.Group,
    entities.Report,
    entities.ReportCategory,
)


class CrudRepo(ABC):
    @abstractmethod
    async def create_one(self, data: core_types.DTO) -> Entity:
        raise NotImplemented

    @abstractmethod
    async def get_one(self, filter_by: dict) -> Entity:
        raise NotImplemented

    @abstractmethod
    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                       slice_from: int = None, slice_to: int = None):
        raise NotImplemented

    @abstractmethod
    async def update_one(self, data: core_types.DTO, filter_by: dict) -> Entity:
        raise NotImplemented

    @abstractmethod
    async def delete_one(self, filter_by: dict) -> core_types.Id_:
        raise NotImplemented


class GroupRepo(CrudRepo, ABC):

    async def overwrite_linked_sheet(self, instance: entities.Group, data: entities.SheetCreate) -> None:
        raise NotImplemented

    async def get_group_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        raise NotImplemented


class ReportRepo(CrudRepo, ABC):
    repo = postgres.ReportRepo()

    async def overwrite_linked_sheet(self, instance: entities.Report, data: entities.SheetCreate) -> None:
        raise NotImplemented


class WireRepo(CrudRepo, ABC):

    async def get_wire_dataframe(self, filter_by: dict, order_by: core_types.OrderBy = None) -> pd.DataFrame:
        raise NotImplemented
