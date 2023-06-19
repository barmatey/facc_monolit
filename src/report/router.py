from fastapi import APIRouter, Depends

from .. import core_types
from .service import Service, BaseService, BalanceService
from . import schema, enums, entities

router_report = APIRouter(
    prefix="/report",
    tags=['Report']
)


def get_service(category: enums.Category) -> Service:
    pass


@router_report.post("/group")
async def create_group(data: schema.GroupCreateSchema) -> core_types.Id_:
    pass


@router_report.get("/group/{group_id}")
async def retrieve_group(group_id: core_types.Id_, service: Service = Depends(BaseService)) -> schema.GroupRetrieveSchema:
    group: entities.GroupRetrieve = await service.retrieve_group(group_id)
    group: schema.GroupRetrieveSchema = schema.GroupRetrieveSchema.from_group_retrieve_entity(group)
    return group


@router_report.delete("/group/{group_id}")
async def delete_group() -> core_types.Id_:
    pass
