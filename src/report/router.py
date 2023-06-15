from fastapi import APIRouter
from fastapi.responses import JSONResponse
from loguru import logger

from .. import core_types
from . import schema
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
async def create_group():
    pass


@router_group.get("/{id_}")
async def retrieve_group():
    pass


@router_group.delete("/{id_}")
async def delete_group():
    pass


@router_group.get("/")
async def retrieve_group_list():
    pass


