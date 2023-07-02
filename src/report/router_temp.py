import enum

import loguru
import pandas as pd
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
    source: entities.Source = await repo.retrieve({"id": id_})
    return source


@router.get("/bulk")
async def retrieve_bulk():
    sources = await repo.retrieve_bulk_as_dicts(filter_by={'title': 'string'}, order_by=['title', 'id'])
    return sources


@router.delete("/delete")
async def delete():
    deleted_id = await repo.delete(filter_by={"id": 18})
    return deleted_id
