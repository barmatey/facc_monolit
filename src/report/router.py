import enum

import loguru
from fastapi import APIRouter, Depends
from loguru import logger

import helpers
from .. import core_types
from .service import Service, BaseService, BalanceService
from . import schema, enums, entities


class LinkedService(enum.Enum):
    BALANCE = BalanceService


def get_service(category: enums.CategoryLiteral) -> Service:
    return LinkedService[category].value()


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
async def create_group(data: schema.GroupCreateSchema) -> schema.GroupSchema:
    service = get_service(data.category)
    group = await service.create_group(data)
    loguru.logger.debug(f'{group}')
    return group


@router_group.get("/{group_id}")
async def retrieve_group(group_id: core_types.Id_,
                         service: Service = Depends(BaseService)) -> schema.GroupSchema:
    group: entities.Group = await service.retrieve_group(group_id)
    return group


@router_group.get("/")
async def retrieve_group_list(
        category: enums.CategoryLiteral = None,
        service: Service = Depends(BaseService)) -> list[schema.GroupSchema]:
    groups: list[schema.GroupSchema] = await service.retrieve_group_list(category_id=get_category_id(category))
    return groups


@router_group.delete("/{group_id}")
async def delete_group(group_id: core_types.Id_, service: Service = Depends(BaseService)) -> core_types.Id_:
    deleted_id = await service.delete_group(group_id)
    return deleted_id


router_report = APIRouter(
    prefix="/report",
    tags=['Report'],
)


@router_report.post("/")
async def create_report(data: schema.ReportCreateSchema) -> schema.ReportSchema:
    service = get_service(data.category)
    report = await service.create_report(data)
    return report


@router_report.get("/{report_id}")
async def retrieve_report(report_id: core_types.Id_, service: Service = Depends(BaseService)) -> schema.ReportSchema:
    report = await service.retrieve_report(report_id)
    return report


@router_report.get("/")
@helpers.async_timeit
async def retrieve_report_list(category: enums.CategoryLiteral = None,
                               service: Service = Depends(BaseService)) -> list[schema.ReportSchema]:
    reports = await service.retrieve_report_list(category_id=get_category_id(category))
    return reports


@router_report.delete("/{report_id}")
async def delete_report(report_id: core_types.Id_,
                        service: Service = Depends(BaseService)) -> core_types.Id_:
    deleted_id = await service.delete_report(report_id)
    return deleted_id


router_category = APIRouter(
    prefix="/category",
    tags=['Category'],
)


@router_category.get("/")
async def retrieve_report_categories(service: BaseService = Depends(BaseService)) -> list[schema.ReportCategorySchema]:
    result = await service.retrieve_report_categories()
    return result
