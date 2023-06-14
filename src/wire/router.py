import pandas as pd
from fastapi import APIRouter, File, UploadFile
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
async def create_source(data: schema.CreateSourceForm) -> core_types.Id_:
    source_id = await service.SourceService().create_source(data)
    return source_id


@router.get("/{id_}")
async def retrieve_source(id_: core_types.Id_) -> schema.Source:
    source = await service.SourceService().retrieve_source(id_)
    return source


@router.delete("/{id_}")
async def delete_source(id_: core_types.Id_) -> int:
    await service.SourceService().delete_source(id_)
    return 1


@router.get("/")
async def list_source():
    pass


@router.post("/{id_}")
async def bulk_append_wire_from_csv(id_: core_types.Id_, file: UploadFile) -> int:
    df = pd.read_csv(file.file, parse_dates=['date'])
    schema.WireSchema.validate(df)
    df = await schema.WireSchema.drop_extra_columns(df)
    await service.WireService().bulk_append_wire_from_csv(id_, df)
    return 1
