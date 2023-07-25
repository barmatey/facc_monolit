from abc import ABC, abstractmethod
import pandas as pd
import typing

import pydantic

Entity = typing.TypeVar("Entity")
DTO = typing.Union[dict, pydantic.BaseModel]
OrderBy = str | list[str]


class Model(ABC):
    @abstractmethod
    def to_entity(self, **kwargs) -> Entity:
        pass


class RepositoryCreateOne(ABC):
    @abstractmethod
    async def create_one_return_model(self, data: DTO) -> Model:
        pass

    @abstractmethod
    async def create_one_return_entity(self, data: DTO) -> Entity:
        pass

    @abstractmethod
    async def s_create_one_return_model(self, session, data: DTO) -> Model:
        pass

    @abstractmethod
    async def s_create_one_return_entity(self, session, data: DTO) -> Entity:
        pass


class RepositoryCreateMany(ABC):
    @abstractmethod
    async def create_many(self, data: list[DTO]) -> None:
        pass

    @abstractmethod
    async def create_many_return_models(self, data: list[DTO]) -> list[Model]:
        pass

    @abstractmethod
    async def create_many_return_entities(self, data: list[DTO]) -> list[Entity]:
        pass

    @abstractmethod
    async def create_many_return_fields(self, data: list[DTO], fields=list[str], scalars=False) -> list:
        pass

    @abstractmethod
    async def s_create_many(self, session, data: list[DTO]) -> None:
        pass

    @abstractmethod
    async def s_create_many_return_models(self, session, data: list[DTO]) -> list[Model]:
        pass

    @abstractmethod
    async def s_create_many_return_entities(self, session, data: list[DTO]) -> list[Entity]:
        pass

    @abstractmethod
    async def s_create_many_return_fields(self, session, data: list[DTO], fields=list[str], scalars=False) -> list:
        pass


class RepositoryGetOne(ABC):
    @abstractmethod
    async def get_one_as_model(self, filter_by: dict) -> Model:
        pass

    @abstractmethod
    async def get_one_as_entity(self, filter_by: dict) -> Entity:
        pass


class RepositoryGetMany(ABC):
    @abstractmethod
    async def get_many_as_models(self, filter_by: dict, order_by: OrderBy = None, asc=True) -> list[Model]:
        pass

    @abstractmethod
    async def get_many_as_entities(self, filter_by: dict, order_by: OrderBy = None, asc=True) -> list[Entity]:
        pass

    @abstractmethod
    async def get_many_as_frame(self, filter_by: dict, order_by: OrderBy = None, asc=True) -> pd.DataFrame:
        pass

    @abstractmethod
    async def get_many_as_dicts(self, filter_by: dict, order_by: OrderBy = None, asc=True) -> list[dict]:
        pass

    @abstractmethod
    async def s_get_many_as_models(self, session, filter_by: dict, order_by: OrderBy = None, asc=True) -> list[Model]:
        pass

    @abstractmethod
    async def s_get_many_as_entities(self, session, filter_by: dict, order_by: OrderBy = None, asc=True) -> list[
        Entity]:
        pass

    @abstractmethod
    async def s_get_many_as_frame(self, session, filter_by: dict, order_by: OrderBy = None, asc=True) -> pd.DataFrame:
        pass

    @abstractmethod
    async def s_get_many_as_dicts(self, session, filter_by: dict, order_by: OrderBy = None, asc=True) -> list[dict]:
        pass


class RepositoryUpdateOne(ABC):

    @abstractmethod
    async def update_one(self, data: DTO, search_keys: list[str]) -> None:
        pass

    @abstractmethod
    async def update_one_return_model(self, data: DTO, search_keys: list[str]) -> Model:
        pass

    @abstractmethod
    async def update_one_return_entity(self, data: DTO, search_keys: list[str]) -> Entity:
        pass


class RepositoryUpdateMany(ABC):

    @abstractmethod
    async def update_many(self, data: list[DTO], search_keys: list[str]) -> None:
        pass

    @abstractmethod
    async def update_many_return_models(self, data: list[DTO], search_keys: list[str]) -> list[Model]:
        pass

    @abstractmethod
    async def update_many_return_entities(self, data: list[DTO], search_keys: list[str]) -> list[Entity]:
        pass

    @abstractmethod
    async def update_many_return_fields(self, data: list[DTO], search_keys: list[str],
                                        fields=list[str], scalars=False) -> list:
        pass

    @abstractmethod
    async def s_update_many(self, session, data: list[DTO], search_keys: list[str]) -> None:
        pass

    @abstractmethod
    async def s_update_many_return_models(self, session, data: list[DTO], search_keys: list[str]) -> list[Model]:
        pass

    @abstractmethod
    async def s_update_many_return_entities(self, session, data: list[DTO], search_keys: list[str]) -> list[Entity]:
        pass

    @abstractmethod
    async def s_update_many_return_fields(self, session, data: list[DTO], search_keys: list[str],
                                          fields=list[str], scalars=False) -> list:
        pass


class RepositoryUpdateBulk(ABC):
    @abstractmethod
    async def update_bulk(self, data: DTO, filter_by: dict) -> None:
        pass


class RepositoryDeleteOne(ABC):
    @abstractmethod
    async def delete_one(self, filter_by: dict) -> None:
        pass

    @abstractmethod
    async def delete_one_return_model(self, filter_by: dict) -> Model:
        pass

    @abstractmethod
    async def delete_one_return_entity(self, filter_by: dict) -> Entity:
        pass

    @abstractmethod
    async def delete_one_return_fields(self, filter_by: dict) -> list:
        pass


class RepositoryDeleteMany(ABC):
    @abstractmethod
    async def delete_many(self, filter_by: dict) -> None:
        pass

    @abstractmethod
    async def delete_many_return_models(self, filter_by: dict) -> list[Model]:
        pass

    @abstractmethod
    async def delete_many_return_entities(self, filter_by: dict) -> list[Entity]:
        pass

    @abstractmethod
    async def delete_many_return_fields(self, filter_by: dict, fields: list[str], scalars=True) -> list:
        pass


class RepositoryDeleteBulk(ABC):
    @abstractmethod
    async def delete_bulk(self, filter_by: dict) -> None:
        pass
