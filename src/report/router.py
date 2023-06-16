from fastapi import APIRouter
from fastapi.responses import JSONResponse
from loguru import logger

from .. import core_types
from ..repository.group import GroupRepo
from ..repository.report import ReportRepo
from . import schema, schema_output
from . import service

router_report = APIRouter(
    prefix="/report",
    tags=['Report']
)


@router_report.post("/")
async def create_report(data: schema.ReportCreateForm, repo=ReportRepo) -> core_types.Id_:
    id_ = await repo().create(data)
    return id_


@router_report.get("/{id_}")
async def retrieve_report(data: schema.ReportRetrieveForm, repo=ReportRepo) -> schema_output.Report:
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
async def create_group(data: schema.GroupCreateForm, repo=GroupRepo) -> core_types.Id_:
    id_ = await repo().create(data)
    return id_


@router_group.get("/{id_}")
async def retrieve_group(id_: core_types.Id_, repo=GroupRepo) -> schema_output.Group:
    group = await repo().retrieve(id_)
    return group


@router_group.delete("/{id_}")
async def delete_group(id_: core_types.Id_, repo=GroupRepo) -> int:
    await repo().delete(id_)
    return 1


@router_group.get("/")
async def retrieve_group_list() -> list[schema_output.Group]:
    pass
