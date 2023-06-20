import loguru
import pandas as pd
from fastapi import APIRouter, UploadFile, Depends


from ..repository.source import SourceRepo
from ..repository.wire import WireRepo
from .. import core_types
from ..wire import entities
from ..wire import schema

router_wire = APIRouter(
    prefix="/source-db",
    tags=['SourceDB']
)


@router_wire.post("/")
async def create_source(data: entities.SourceCreateData, repo: SourceRepo = Depends(SourceRepo)) -> core_types.Id_:
    source_id = await repo.create(data)
    return source_id


@router_wire.get("/{id_}")
async def retrieve_source(id_: core_types.Id_, repo: SourceRepo = Depends(SourceRepo)) -> schema.Source:
    raise NotImplemented


@router_wire.delete("/{id_}")
async def delete_source(id_: core_types.Id_, repo: SourceRepo = Depends(SourceRepo)) -> int:
    raise NotImplemented


@router_wire.get("/")
async def list_source():
    raise NotImplemented


@router_wire.post("/{id_}")
async def bulk_append_wire_from_csv(id_: core_types.Id_, file: UploadFile, repo: WireRepo = Depends(WireRepo)) -> int:
    df = pd.read_csv(file.file, parse_dates=['date'])
    df['source_id'] = id_
    await repo.bulk_create_wire(df)
    return 1
