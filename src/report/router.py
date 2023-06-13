from fastapi import APIRouter
from fastapi.responses import JSONResponse
from loguru import logger

from .. import core_types
from . import schema
from . import service

router = APIRouter(
    prefix="/report",
    tags=['Report']
)


@router.post("/create-balance-group/")
async def create_balance_group(data: schema.GroupCreate) -> JSONResponse:
    group_id: core_types.Id_ = await service.BalanceService().create_balance_group(data)
    return JSONResponse(content={"group_id": group_id})
