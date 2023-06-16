import pandas as pd
from fastapi import APIRouter, File, UploadFile, Depends
from fastapi.responses import JSONResponse
from loguru import logger

from .. import entities
from ..repository.source import SourceRepo
from ..repository.wire import WireRepo
from .. import core_types
from . import schema

router = APIRouter(
    prefix="/source-db",
    tags=['SourceDB']
)


@router.post("/")
async def create_source(data: entities.SourceCreateData, repo: SourceRepo = Depends(SourceRepo)) -> core_types.Id_:
    source_id = await repo.create(data)
    return source_id


@router.get("/{id_}")
async def retrieve_source(id_: core_types.Id_, repo: SourceRepo = Depends(SourceRepo)) -> schema.Source:
    source = await repo.retrieve(id_)
    return source


@router.delete("/{id_}")
async def delete_source(id_: core_types.Id_, repo: SourceRepo = Depends(SourceRepo)) -> int:
    await repo.delete(id_)
    return 1


@router.get("/")
async def list_source():
    pass


@router.post("/{id_}")
async def bulk_append_wire_from_csv(id_: core_types.Id_, file: UploadFile, repo: WireRepo = Depends(WireRepo)) -> int:
    df = pd.read_csv(file.file, parse_dates=['date'])
    df['source_id'] = id_
    await repo.bulk_create_wire(df)
    return 1
