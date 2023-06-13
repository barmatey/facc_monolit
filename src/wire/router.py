from fastapi import APIRouter
from fastapi.responses import JSONResponse
from loguru import logger

from .. import core_types
from . import schema
from . import service

router = APIRouter(
    prefix="/source-db",
    tags=['SourceDB']
)


@router.post("/")
async def create_source_db(data: schema.CreateSourceForm) -> core_types.Id_:
    source_id = await service.SourceService().create_source(data)
    return source_id


@router.get("/{id_}")
async def retrieve_source_db(id_: core_types.Id_):
    pass


@router.delete("/{id_}")
async def delete_source_db():
    pass


@router.post("/{id_}")
async def bulk_append_wire():
    pass
