from loguru import logger
from fastapi import APIRouter, Depends

from .. import core_types
from ..repository.report import ReportRepo
from ..sheet import entities
from ..sheet import schema

router_sheet = APIRouter(
    prefix="/sheet",
    tags=['Sheet']
)


@router_sheet.get("/{sheet_id}/")
async def retrieve_sheet(data: schema.SheetRetrieveSchema) -> entities.Sheet:
    logger.debug(data)
    return 22
