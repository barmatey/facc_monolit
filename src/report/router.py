from fastapi import APIRouter
from fastapi.responses import JSONResponse
from loguru import logger

from .. import core_types
from . import schema, schema_output
from . import service

router_report = APIRouter(
    prefix="/report",
    tags=['Report']
)


@router_report.post("/")
async def create_report() -> core_types.Id_:
    pass


@router_report.get("/{id_}")
async def retrieve_report():
    pass


@router_report.delete("/{id_}")
async def delete_report():
    pass


@router_report.get("/")
async def retrieve_report_list():
    pass


router_group = APIRouter(
    prefix="/group",
    tags=['Group']
)


@router_group.post("/")
async def create_group(data: schema.GroupCreateForm) -> core_types.Id_:
    id_ = await service.GroupService().create_group(data)
    return id_


@router_group.get("/{id_}")
async def retrieve_group(id_: core_types.Id_) -> schema_output.Group:
    data = schema.GroupRetrieveForm(id_=id_)
    group = await service.GroupService().retrieve_group(data)
    return group


@router_group.delete("/{id_}")
async def delete_group(id_: core_types.Id_) -> int:
    data = schema.GroupDeleteForm(id_=id_)
    await service.GroupService().delete_group(data)
    return 1


@router_group.get("/")
async def retrieve_group_list() -> list[schema_output.Group]:
    pass
