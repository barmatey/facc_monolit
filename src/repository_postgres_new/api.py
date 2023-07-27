from fastapi import APIRouter

import helpers
from .base import BasePostgres
from src.repository_postgres.group import Group
from src.repository_postgres.sheet import Cell
from loguru import logger

router_dev = APIRouter(
    prefix="/dev",
    tags=['DEV']
)


@router_dev.get("/")
@helpers.async_timeit
async def create_group():
    filter_by = {}
    result = await BasePostgres(Cell, "FRAME").get_many(filter_by)
    return 12
