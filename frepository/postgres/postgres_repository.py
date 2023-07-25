import typing

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession as AS

from .. import repository
from ..repository import DTO, Entity, Model, OrderBy


class PostgresBaseRepo:
    model: Model
    session_maker: typing.Callable


class PostgresCreateOneRepo(repository.RepositoryCreateOne, PostgresBaseRepo):
    async def create_one_return_model(self, data: DTO) -> Model:
        raise NotImplemented

    async def create_one_return_entity(self, data: DTO) -> Entity:
        raise NotImplemented

    async def s_create_one_return_model(self, session, data: DTO) -> Model:
        raise NotImplemented

    async def s_create_one_return_entity(self, session, data: DTO) -> Entity:
        raise NotImplemented


class PostgresCreateManyRepo(repository.RepositoryCreateMany, PostgresBaseRepo):
    async def create_many(self, data: list[DTO]) -> None:
        raise NotImplemented

    async def create_many_return_models(self, data: list[DTO]) -> list[Model]:
        raise NotImplemented

    async def create_many_return_entities(self, data: list[DTO]) -> list[Entity]:
        raise NotImplemented

    async def create_many_return_fields(self, data: list[DTO], fields=list[str], scalars=False) -> list:
        raise NotImplemented

    async def s_create_many(self, session, data: list[DTO]) -> None:
        raise NotImplemented

    async def s_create_many_return_models(self, session: AS, data: list[DTO]) -> list[Model]:
        raise NotImplemented

    async def s_create_many_return_entities(self, session: AS, data: list[DTO]) -> list[Entity]:
        raise NotImplemented

    async def s_create_many_return_fields(self, session: AS, data: list[DTO], fields=list[str], scalars=False) -> list:
        raise NotImplemented


class PostgresGetOneRepo(repository.RepositoryGetOne, PostgresBaseRepo):
    async def get_one_as_model(self, filter_by: dict) -> Model:
        raise NotImplemented

    async def get_one_as_entity(self, filter_by: dict) -> Entity:
        raise NotImplemented


class PostgresGetManyRepo(repository.RepositoryGetMany, PostgresBaseRepo):
    async def get_many_as_models(self, filter_by: dict, order_by: OrderBy = None, asc=True) -> list[Model]:
        raise NotImplemented

    async def get_many_as_entities(self, filter_by: dict, order_by: OrderBy = None, asc=True) -> list[Entity]:
        raise NotImplemented

    async def get_many_as_frame(self, filter_by: dict, order_by: OrderBy = None, asc=True) -> pd.DataFrame:
        raise NotImplemented

    async def get_many_as_dicts(self, filter_by: dict, order_by: OrderBy = None, asc=True) -> list[dict]:
        raise NotImplemented

    async def s_get_many_as_models(self, session: AS, filter_by: dict,
                                   order_by: OrderBy = None, asc=True) -> list[Model]:
        raise NotImplemented

    async def s_get_many_as_entities(self, session: AS, filter_by: dict,
                                     order_by: OrderBy = None, asc=True) -> list[Entity]:
        raise NotImplemented

    async def s_get_many_as_frame(self, session: AS, filter_by: dict, order_by: OrderBy = None,
                                  asc=True) -> pd.DataFrame:
        raise NotImplemented

    async def s_get_many_as_dicts(self, session: AS, filter_by: dict, order_by: OrderBy = None, asc=True) -> list[dict]:
        raise NotImplemented


class PostgresUpdateOneRepo(repository.RepositoryUpdateOne, PostgresBaseRepo):

    async def update_one(self, data: DTO, search_keys: list[str]) -> None:
        raise NotImplemented

    async def update_one_return_model(self, data: DTO, search_keys: list[str]) -> Model:
        raise NotImplemented

    async def update_one_return_entity(self, data: DTO, search_keys: list[str]) -> Entity:
        raise NotImplemented


class PostgresUpdateManyRepo(repository.RepositoryUpdateMany, PostgresBaseRepo):

    async def update_many(self, data: list[DTO], search_keys: list[str]) -> None:
        raise NotImplemented

    async def update_many_return_models(self, data: list[DTO], search_keys: list[str]) -> list[Model]:
        raise NotImplemented

    async def update_many_return_entities(self, data: list[DTO], search_keys: list[str]) -> list[Entity]:
        raise NotImplemented

    async def update_many_return_fields(self, data: list[DTO], search_keys: list[str], fields=list[str],
                                        scalars=False) -> list:
        raise NotImplemented

    async def s_update_many(self, session: AS, data: list[DTO], search_keys: list[str]) -> None:
        raise NotImplemented

    async def s_update_many_return_models(self, session: AS, data: list[DTO], search_keys: list[str]) -> list[Model]:
        raise NotImplemented

    async def s_update_many_return_entities(self, session: AS, data: list[DTO], search_keys: list[str]) -> list[Entity]:
        raise NotImplemented

    async def s_update_many_return_fields(self, session: AS, data: list[DTO], search_keys: list[str], fields=list[str],
                                          scalars=False) -> list:
        raise NotImplemented


class PostgresUpdateBulkRepo(repository.RepositoryUpdateBulk, PostgresBaseRepo):
    async def update_bulk(self, data: DTO, filter_by: dict) -> None:
        raise NotImplemented


class PostgresDeleteOneRepo(repository.RepositoryDeleteOne, PostgresBaseRepo):
    async def delete_one(self, filter_by: dict) -> None:
        raise NotImplemented

    async def delete_one_return_model(self, filter_by: dict) -> Model:
        raise NotImplemented

    async def delete_one_return_entity(self, filter_by: dict) -> Entity:
        raise NotImplemented

    async def delete_one_return_fields(self, filter_by: dict) -> list:
        raise NotImplemented


class PostgresDeleteManyRepo(repository.RepositoryDeleteMany, PostgresBaseRepo):
    async def delete_many(self, filter_by: dict) -> None:
        raise NotImplemented

    async def delete_many_return_models(self, filter_by: dict) -> list[Model]:
        raise NotImplemented

    async def delete_many_return_entities(self, filter_by: dict) -> list[Entity]:
        raise NotImplemented

    async def delete_many_return_fields(self, filter_by: dict, fields: list[str], scalars=True) -> list:
        raise NotImplemented


class PostgresDeleteBulkRepo(repository.RepositoryDeleteBulk, PostgresBaseRepo):
    async def delete_bulk(self, filter_by: dict) -> None:
        raise NotImplemented


class PostgresRepository(
    PostgresCreateOneRepo,
    PostgresCreateManyRepo,
    PostgresGetOneRepo,
    PostgresGetManyRepo,
    PostgresUpdateOneRepo,
    PostgresUpdateManyRepo,
    PostgresUpdateBulkRepo,
    PostgresDeleteOneRepo,
    PostgresDeleteManyRepo,
    PostgresDeleteBulkRepo,
):
    pass
