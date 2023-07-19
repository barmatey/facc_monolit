import enum

from fastapi import APIRouter, Depends

import helpers
from .. import core_types
from .service_crud import Service, CategoryService, ReportService, GroupService
from . import schema, enums, entities


class LinkedCategory(enum.Enum):
    BALANCE = 1
    PROFIT = 2
    CASHFLOW = 3


def get_category_id(category: enums.CategoryLiteral | None) -> int | None:
    result = LinkedCategory[category].value if category is not None else None
    return result


router_group = APIRouter(
    prefix="/group",
    tags=['Group']
)


@router_group.post("/")
@helpers.async_timeit
async def create_group(data: schema.GroupCreateSchema,
                       service: Service = Depends(GroupService)) -> schema.GroupSchema:
    group = await service.create(data)
    return group


@router_group.get("/{group_id}")
@helpers.async_timeit
async def retrieve_group(group_id: core_types.Id_,
                         service: Service = Depends(GroupService)) -> schema.GroupSchema:
    group: entities.Group = await service.retrieve({"id": group_id})
    return group


@router_group.get("/")
@helpers.async_timeit
async def retrieve_group_list(
        category: enums.CategoryLiteral = None,
        service: Service = Depends(GroupService)) -> list[schema.GroupSchema]:
    groups: list[schema.GroupSchema] = await service.retrieve_bulk({})
    return groups


@router_group.patch("/{group_id}/total-recalculate")
@helpers.async_timeit
async def total_recalculate(group_id: core_types.Id_, service: GroupService = Depends(GroupService)) -> entities.Group:
    group = await service.retrieve({"id": group_id})
    updated = await service.total_recalculate(group)
    return updated


@router_group.delete("/{group_id}")
@helpers.async_timeit
async def delete_group(group_id: core_types.Id_, service: Service = Depends(GroupService)) -> core_types.Id_:
    deleted_id = await service.delete({"id": group_id})
    return deleted_id


router_report = APIRouter(
    prefix="/report",
    tags=['Report'],
)


@router_report.post("/")
@helpers.async_timeit
async def create_report(data: schema.ReportCreateSchema,
                        service: Service = Depends(ReportService)) -> schema.ReportSchema:
    report = await service.create(data)
    return report


@router_report.get("/{report_id}")
@helpers.async_timeit
async def retrieve_report(report_id: core_types.Id_, service: Service = Depends(ReportService)) -> schema.ReportSchema:
    report = await service.retrieve({"id": report_id})
    return report


@router_report.get("/")
@helpers.async_timeit
async def retrieve_report_list(category: enums.CategoryLiteral = None,
                               service: Service = Depends(ReportService)) -> list[schema.ReportSchema]:
    reports = await service.retrieve_bulk({"category_id": get_category_id(category)})
    return reports


@router_report.delete("/{report_id}")
@helpers.async_timeit
async def delete_report(report_id: core_types.Id_,
                        service: Service = Depends(ReportService)) -> core_types.Id_:
    deleted_id = await service.delete({"id": report_id})
    return deleted_id


router_category = APIRouter(
    prefix="/category",
    tags=['Category'],
)


@router_category.post("/")
async def create(data: schema.ReportCategoryCreateSchema,
                 service: Service = Depends(CategoryService)) -> entities.ReportCategory:
    category: entities.ReportCategory = await service.create(data)
    return category


@router_category.get("/")
async def retrieve_report_categories(service: Service = Depends(CategoryService)
                                     ) -> list[schema.ReportCategorySchema]:
    result: list[entities.ReportCategory] = await service.retrieve_bulk({})
    categories = [category.value for category in result]
    return categories
