from fastapi import APIRouter
from .base import Repository
from src.repository_postgres.group import Group
from loguru import logger

router_dev = APIRouter(
    prefix="/dev",
    tags=['DEV']
)


@router_dev.get("/")
async def create_group():
    filter_by = {}
    result = await Repository(Group, "ENTITY").get_one(filter_by)
    logger.debug(result)
    return 12
