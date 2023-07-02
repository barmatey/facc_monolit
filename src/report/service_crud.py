from abc import ABC, abstractmethod

import pandas as pd
from pydantic import BaseModel

from src import core_types
from . import entities, schema
from .repository_new import Repository


class Service(ABC):
    @abstractmethod
    async def create(self, data: BaseModel) -> BaseModel:
        pass

    @abstractmethod
    async def retrieve(self, **kwargs) -> BaseModel:
        pass

    @abstractmethod
    async def retrieve_bulk(self, **kwargs) -> list[BaseModel]:
        pass

    @abstractmethod
    async def partial_update(self, data: BaseModel, **kwargs) -> BaseModel:
        pass

    @abstractmethod
    async def delete(self, **kwargs) -> core_types.Id_:
        pass


class BaseService(Service):

    def __init__(self, repo: Repository):
        self.repo = repo

    async def create(self, data: BaseModel) -> BaseModel:
        raise NotImplemented

    async def retrieve(self, **kwargs) -> BaseModel:
        raise NotImplemented

    async def retrieve_bulk(self, **kwargs) -> list[BaseModel]:
        raise NotImplemented

    async def partial_update(self, data: BaseModel, **kwargs) -> BaseModel:
        raise NotImplemented

    async def delete(self, **kwargs) -> core_types.Id_:
        raise NotImplemented


class CategoryService(BaseService):
    pass


