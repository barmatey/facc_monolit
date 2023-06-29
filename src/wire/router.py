import loguru
import pandas as pd
from fastapi import APIRouter, UploadFile, Depends

from repository_postgres.wire import WireRepo
from src import core_types
from . import entities
from . import schema
from .service import Service, ServiceSource

router_wire = APIRouter(
    prefix="/source-db",
    tags=['SourceDB']
)


@router_wire.post("/", response_model=schema.SourceSchema)
async def create_source(data: schema.SourceCreateSchema, service: Service = Depends(ServiceSource)):
    return await service.create(data)


@router_wire.get("/{source_id}", response_model=schema.SourceSchema)
async def retrieve_source(source_id: core_types.Id_, service: Service = Depends(ServiceSource)):
    return await service.retrieve(source_id)


@router_wire.delete("/{source_id}", response_model=int)
async def delete_source(source_id: core_types.Id_, service: Service = Depends(ServiceSource)):
    await service.delete(source_id)


@router_wire.get("/", response_model=list[schema.SourceSchema])
async def list_source(service: Service = Depends(ServiceSource)):
    result = await service.retrieve_list()
    return result


@router_wire.post("/{id_}")
async def bulk_append_wire_from_csv(id_: core_types.Id_, file: UploadFile, repo: WireRepo = Depends(WireRepo)) -> int:
    df = pd.read_csv(file.file, parse_dates=['date'])
    df['source_id'] = id_
    await repo.bulk_create_wire(df)
    return 1
