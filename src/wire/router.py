import pandas as pd
from fastapi import APIRouter, UploadFile, Depends

import helpers
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
@helpers.async_timeit
async def bulk_append_wire_from_csv(source_id: core_types.Id_, file: UploadFile,) -> int:
    df = pd.read_csv(file.file, parse_dates=['date'])
    df['date'] = pd.to_datetime(df['date'], utc=True)
    # await repo.bulk_create_wire(source_id, df)
    return 1


router_wire = APIRouter(
    prefix='/wire',
    tags=['Wire'],
)


@router_wire.post("/")
@helpers.async_timeit
async def create(data: schema.WireCreateSchema, service: ServiceWire = Depends(ServiceWire)) -> schema.WireSchema:
    return await service.create(data)


@router_wire.get("/")
@helpers.async_timeit
async def retrieve_list(source_id: core_types.Id_,
                        date: pd.Timestamp = None,
                        sender: float = None,
                        receiver: float = None,
                        debit: float = None,
                        credit: float = None,
                        subconto_first: str = None,
                        subconto_second: str = None,
                        comment: str = None,
                        paginate_from: int = None,
                        paginate_to: int = None,
                        service: ServiceWire = Depends(ServiceWire)) -> list[schema.WireSchema]:
    filter_by = {
        "source_id": source_id,
        "date": date,
        "sender": sender,
        "receiver": receiver,
        "debit": debit,
        "credit": credit,
        "subconto_first": subconto_first,
        "subconto_second": subconto_second,
        "comment": comment,
    }

    retrieve_params = schema.WireBulkRetrieveSchema(
        filter_by=filter_by,
        order_by=['date', 'sender', 'receiver', ],
        ascending=True,
        paginate_from=paginate_from,
        paginate_to=paginate_to,
    )
    return await service.retrieve_list(retrieve_params)


@router_wire.patch("/{wire_id}")
@helpers.async_timeit
async def update(wire_id: core_types.Id_, data: schema.WireCreateSchema,
                 service: ServiceWire = Depends(ServiceWire)) -> schema.WireSchema:
    filter_by = {"id": wire_id}
    updated = await service.update(filter_by, data)
    return updated


@router_wire.delete("/{wire_id}")
@helpers.async_timeit
async def delete(wire_id: core_types.Id_, service: ServiceWire = Depends(ServiceWire)) -> int:
    filter_by = {"id": wire_id}
    await service.delete(filter_by=filter_by)
    return 1
