import loguru
from fastapi import APIRouter, Depends

from src import core_types
from src import helpers
from . import service

router = APIRouter(
    prefix="/publisher",
    tags=['Publisher'],
)


@router.patch("/{source_id}")
@helpers.async_timeit
async def total_update_source(source_id: core_types.Id_) -> int:
    loguru.logger.debug(source_id)
    await service.total_recalculate_entities_linked_with_source(source_id)
    return 1
