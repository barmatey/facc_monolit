import enum

import loguru
from fastapi import APIRouter, Depends
from loguru import logger

import helpers
from repository_postgres_new import SourceRepo
from wire import schema, entities

router = APIRouter(
    prefix="/temp",
    tags=['A_TEMP']
)

repo = SourceRepo()


@router.post("/")
async def create(data: schema.SourceCreateSchema):
    source: entities.Source = await repo.create(data)
    return source


@router.post("/create-bulk")
async def create_bulk(data: list[schema.SourceCreateSchema]):
    source: list[int] = await repo.create_bulk(data)
    return source


@router.get("/:id_")
async def retrieve(id_: int):
    source: entities.Source = await repo.retrieve(id=id_)
    return source
