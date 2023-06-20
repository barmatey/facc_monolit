import enum

from fastapi import APIRouter, Depends
from loguru import logger

from .. import core_types
from .service import Service, BaseService, BalanceService
from . import schema, enums, entities

router_report = APIRouter(
    prefix="/report",
    tags=['Report']
)


class LinkedService(enum.Enum):
    BALANCE = BalanceService


def get_service(category: enums.CategoryLiteral) -> Service:
    return LinkedService[category].value()


@router_report.post("/group")
async def create_group(data: schema.GroupCreateSchema) -> core_types.Id_:
    service = get_service(data.category)
    group_id = await service.create_group(data)
    return group_id


@router_report.get("/group/{group_id}")
async def retrieve_group(group_id: core_types.Id_,
                         service: Service = Depends(BaseService)) -> schema.GroupRetrieveSchema:
    group: entities.Group = await service.retrieve_group(group_id)
    group: schema.GroupRetrieveSchema = schema.GroupRetrieveSchema.from_group_retrieve_entity(group)
    return group


@router_report.delete("/group/{group_id}")
async def delete_group(group_id: core_types.Id_, service: Service = Depends(BaseService)) -> core_types.Id_:
    deleted_id = await service.delete_group(group_id)
    return deleted_id


@router_report.post("/finrep")
async def create_finrep(data: schema.ReportCreateSchema) -> core_types.Id_:
    service = get_service(data.category)
    report_id = await service.create_report(data)
    return report_id
