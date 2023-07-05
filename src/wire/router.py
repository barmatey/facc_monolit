import pandas as pd
from fastapi import APIRouter, UploadFile, Depends

from repository_postgres import WireRepo
from src import core_types
from . import schema
from .service import Service, ServiceSource, ServiceWire

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
    retrieve_params = schema.SourceBulkRetrieveSchema(filter_by={})
    result = await service.retrieve_list(retrieve_params)
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
async def retrieve_list(source_id: core_types.Id_,
                        paginate_from: int = None, paginate_to: int = None,
                        service: ServiceWire = Depends(ServiceWire)) -> list[schema.WireSchema]:
    retrieve_params = schema.WireBulkRetrieveSchema(
        filter_by={"source_id": source_id},
        order_by='date',
        ascending=True,
        paginate_from=paginate_from,
        paginate_to=paginate_to,
    )
    return await service.retrieve_list(retrieve_params)
