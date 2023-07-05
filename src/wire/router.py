import pandas as pd
from fastapi import APIRouter, UploadFile, Depends

from repository_postgres import WireRepo
from src import core_types
from . import schema
from .service import Service, ServiceSource

router_source = APIRouter(
    prefix="/source-db",
    tags=['SourceDB']
)


@router_source.post("/", response_model=schema.SourceSchema)
async def create_source(data: schema.SourceCreateSchema, service: Service = Depends(ServiceSource)):
    return await service.create(data)


@router_source.get("/{source_id}", response_model=schema.SourceSchema)
async def retrieve_source(source_id: core_types.Id_, service: Service = Depends(ServiceSource)):
    return await service.retrieve(filter_by={"id": source_id})


@router_source.delete("/{source_id}", response_model=int)
async def delete_source(source_id: core_types.Id_, service: Service = Depends(ServiceSource)):
    await service.delete(filter_by={"id": source_id})
    return 1


@router_source.get("/", response_model=list[schema.SourceSchema])
async def list_source(service: Service = Depends(ServiceSource)):
    result = await service.retrieve_list(filter_by={})
    return result


@router_source.post("/{source_id}")
async def bulk_append_wire_from_csv(source_id: core_types.Id_, file: UploadFile,
                                    repo: WireRepo = Depends(WireRepo)) -> int:
    df = pd.read_csv(file.file, parse_dates=['date'])
    await repo.bulk_create_wire(source_id, df)
    return 1


router_wire = APIRouter(
    prefix='/wire',
    tags=['Wire'],
)


@router_wire.get("/")
async def retrieve_list(source_id: core_types.Id_, page: int = None) -> list[schema.WireSchema]:
    raise NotImplemented
