from abc import ABC, abstractmethod
import pandas as pd
import pydantic

from .my_dto import DTO
from .my_filter import MyFilter


class Repository(ABC):

    @abstractmethod
    async def create_one(self, data: DTO) -> DTO:
        pass

    @abstractmethod
    async def create_many(self, data: list[DTO]) -> list[DTO]:
        pass

    @abstractmethod
    async def get_one(self, filter_by: MyFilter) -> DTO:
        pass

    @abstractmethod
    async def get_many(self, filter_by: MyFilter) -> list[DTO]:
        pass

    @abstractmethod
    async def get_many_as_frame(self, filter_by: MyFilter) -> pd.DataFrame:
        pass

    @abstractmethod
    async def get_many_as_dicts(self, filter_by: MyFilter) -> list[dict]:
        pass

    @abstractmethod
    async def get_many_as_records(self, filter_by: MyFilter) -> list[tuple]:
        pass


class RepositoryPostgres(Repository):
    class Meta:
        entity: DTO

    async def create_one(self, data: DTO) -> DTO:
        pass

    async def create_many(self, data: list[DTO]) -> list[DTO]:
        pass

    async def get_one(self, filter_by: MyFilter) -> DTO:
        pass

    async def get_many(self, filter_by: MyFilter) -> list[DTO]:
        pass

    async def get_many_as_frame(self, filter_by: MyFilter) -> pd.DataFrame:
        pass

    async def get_many_as_dicts(self, filter_by: MyFilter) -> list[dict]:
        pass

    async def get_many_as_records(self, filter_by: MyFilter) -> list[tuple]:
        pass


"""
Test
"""


class TestModel(pydantic.BaseModel):
    field: int


class Repo(RepositoryPostgres):
    pass


async def foo():
    my_repo = Repo()
    entity: TestModel = await my_repo.get_one(MyFilter())
