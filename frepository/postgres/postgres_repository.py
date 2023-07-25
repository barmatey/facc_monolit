import pandas as pd

from .. import repository
from ..repository import DTO, Entity, Model


class PostgresCreateOneRepo(repository.RepositoryCreateOne):
    async def create_one_return_model(self, data: DTO) -> Model:
        raise NotImplemented

    async def create_one_return_entity(self, data: DTO) -> Entity:
        raise NotImplemented


class PostgresCreateManyRepo(repository.RepositoryCreateMany):
    async def create_many_return_models(self, data: list[DTO]) -> list[Model]:
        raise NotImplemented

    async def create_many_return_entities(self, data: list[DTO]) -> list[Entity]:
        raise NotImplemented

    async def create_many_return_fields(self, data: list[DTO], fields=list[str], scalars=False) -> list:
        raise NotImplemented


class PostgresGetOneRepo(repository.RepositoryGetOne):
    async def get_one_as_model(self, filter_by: dict) -> Model:
        raise NotImplemented

    async def get_one_as_entity(self, filter_by: dict) -> Entity:
        raise NotImplemented


class PostgresGetManyRepo(repository.RepositoryGetMany):
    async def get_many_as_models(self, filter_by: dict) -> list[Model]:
        raise NotImplemented

    async def get_many_as_entities(self, filter_by: dict) -> list[Entity]:
        raise NotImplemented

    async def get_many_as_frame(self, filter_by: dict) -> pd.DataFrame:
        raise NotImplemented

    async def get_many_as_dicts(self, filter_by: dict) -> list[dict]:
        raise NotImplemented


class PostgresUpdateOneRepo(repository.RepositoryUpdateOne):
    async def update_one_return_model(self, data: DTO, search_keys: list[str]) -> Model:
        raise NotImplemented

    async def update_one_return_entity(self, data: DTO, search_keys: list[str]) -> Entity:
        raise NotImplemented


class PostgresUpdateManyRepo(repository.RepositoryUpdateMany):
    async def update_many_return_models(self, data: list[DTO], search_keys: list[str]) -> list[Model]:
        raise NotImplemented

    async def update_many_return_entities(self, data: list[DTO], search_keys: list[str]) -> list[Entity]:
        raise NotImplemented

    async def update_many_return_fields(self, data: list[DTO], search_keys: list[str], fields=list[str],
                                        scalars=False) -> list:
        raise NotImplemented


class PostgresDeleteOneRepo(repository.RepositoryDeleteOne):
    async def delete_one(self, filter_by: dict) -> None:
        raise NotImplemented

    async def delete_one_return_model(self, filter_by: dict) -> Model:
        raise NotImplemented

    async def delete_one_return_entity(self, filter_by: dict) -> Entity:
        raise NotImplemented

    async def delete_one_return_fields(self, filter_by: dict) -> list:
        raise NotImplemented


class PostgresDeleteManyRepo(repository.RepositoryDeleteMany):
    async def delete_many(self, filter_by: dict) -> None:
        raise NotImplemented

    async def delete_many_return_models(self, filter_by: dict) -> list[Model]:
        raise NotImplemented

    async def delete_many_return_entities(self, filter_by: dict) -> list[Entity]:
        raise NotImplemented

    async def delete_many_return_fields(self, filter_by: dict, fields: list[str], scalars=True) -> list:
        raise NotImplemented


class PostgresRepository(
    PostgresCreateOneRepo,
    PostgresCreateManyRepo,
    PostgresGetOneRepo,
    PostgresGetManyRepo,
    PostgresUpdateOneRepo,
    PostgresUpdateManyRepo,
    PostgresDeleteOneRepo,
    PostgresDeleteManyRepo,
):
    pass
