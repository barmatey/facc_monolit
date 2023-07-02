from abc import ABC, abstractmethod

import pandas as pd
from pydantic import BaseModel

from src import core_types


class Repository(ABC):
    @abstractmethod
    async def create(self, data: BaseModel) -> BaseModel:
        pass

    @abstractmethod
    async def create_bulk(self, data: list[BaseModel] | list[dict]) -> list[core_types.Id_]:
        pass

    @abstractmethod
    async def retrieve(self, **kwargs) -> BaseModel:
        pass

    @abstractmethod
    async def retrieve_bulk(self, **kwargs) -> list[BaseModel]:
        pass

    @abstractmethod
    async def retrieve_bulk_as_dataframe(self, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    async def partial_update(self, data: BaseModel, **kwargs) -> BaseModel:
        pass

    @abstractmethod
    async def partial_update_bulk(self, data: list[BaseModel] | list[dict], **kwargs) -> list[core_types.Id_]:
        pass

    @abstractmethod
    async def delete(self, **kwargs) -> core_types.Id_:
        pass
