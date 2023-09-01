import pandas as pd
import pydantic

from src import core_types
from src.core_types import OrderBy, DTO
from . import repository, entities


class CrudService:

    def __init__(self, crud_repo: repository.RepositoryCrud):
        self.__crud_repo = crud_repo

    async def create_one(self, data: pydantic.BaseModel) -> entities.Entity:
        return await self.__crud_repo.create_one(data)

    async def create_many(self, data: list[DTO]) -> None:
        await self.__crud_repo.create_many(data)

    async def get_one(self, filter_by: dict) -> entities.Entity:
        return await self.__crud_repo.get_one(filter_by)

    async def get_many(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                       slice_from: int = None, slice_to: int = None) -> list[entities.Entity]:
        return await self.__crud_repo.get_many(filter_by, order_by, asc, slice_from, slice_to)

    async def get_uniques(self, columns_by: list[str], filter_by: dict,
                          order_by: OrderBy = None, asc=True,) -> list[dict]:
        return await self.__crud_repo.get_uniques(columns_by, filter_by, order_by, asc)

    async def get_many_as_frame(self, filter_by: dict, order_by: OrderBy = None, asc=True,
                                slice_from: int = None, slice_to: int = None) -> pd.DataFrame:
        return await self.__crud_repo.get_many_as_frame(filter_by, order_by, asc, slice_from, slice_to)

    async def update_one(self, data: DTO, filter_by: dict, ) -> entities.Entity:
        return await self.__crud_repo.update_one(data, filter_by)

    async def update_many_via_id(self, data: list[DTO]) -> None:
        await self.__crud_repo.update_many_via_id(data)

    async def delete_one(self, filter_by: dict) -> entities.Entity:
        return await self.__crud_repo.delete_one(filter_by)

    async def delete_many(self, filter_by: dict) -> None:
        await self.__crud_repo.delete_many(filter_by)


class PlanItemService(CrudService):

    async def create_many_from_wire_df(self, source_id: core_types.Id_, wire_df: pd.DataFrame) -> None:
        columns = ['sender', 'receiver', 'sub1', 'sub2']
        existed_plan_items = (await self.get_many_as_frame({"source_id": source_id}))[columns]

        new_plan_items = (
            wire_df
            .rename({"subconto_first": "sub1", "subconto_second": "sub2"}, axis=1)[columns]
            .drop_duplicates(ignore_index=True)
        )

        plan_items = pd.concat([existed_plan_items, new_plan_items])
        plan_items['source_id'] = source_id
        duplicated = plan_items.duplicated(keep=False)
        plan_items = plan_items.loc[~duplicated]

        if not plan_items.empty:
            await self.create_many(plan_items.to_dict(orient='records'))
