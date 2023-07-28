import typing
from abc import ABC, abstractmethod

import pandas as pd
import pydantic

from src.core_types import OrderBy, Id_
from . import repository, entities


class Service:

    def __init__(self, crud_repo: repository.RepositoryCrud):
        self.__crud_repo = crud_repo

    async def create_one(self, data: pydantic.BaseModel) -> entities.Entity:
        return await self.__crud_repo.create_one(data)

    async def get_one(self, filter_by: dict) -> entities.Entity:
        return await self.__crud_repo.get_one(filter_by)

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                       slice_from: int = None, slice_to: int = None) -> list[entities.Entity]:
        return await self.__crud_repo.get_many(filter_by, order_by, asc, slice_from, slice_to)

    async def get_many_as_frame(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                                slice_from: int = None, slice_to: int = None) -> pd.DataFrame:
        return await self.__crud_repo.get_many_as_frame(filter_by, order_by, asc, slice_from, slice_to)

    async def update_one(self, data: pydantic.BaseModel, filter_by: dict,) -> entities.Entity:
        return await self.__crud_repo.update_one(data, filter_by)

    async def delete_one(self, filter_by: dict) -> Id_:
        return await self.__crud_repo.delete_one(filter_by)
