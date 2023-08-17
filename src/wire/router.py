import datetime

import pandas as pd
from fastapi import APIRouter, UploadFile, Depends
from loguru import logger

from src import db
from src import core_types, helpers
from src.messagebus import messagebus as msgbus
from src.repository_postgres_new import SourceRepoPostgres, WireRepoPostgres

from . import entities, schema, messagebus, events
from .service import CrudService

router_source = APIRouter(
    prefix="/source-db",
    tags=['SourceDB']
)


@router_source.post("/")
async def create_source(data: events.SourceCreated, get_asession=Depends(db.get_async_session)) -> entities.Source:
    async with get_asession as session:
        result = await msgbus.handle(data, session)
        source: entities.Source = result[events.SourceCreated]
        await session.commit()
        return source


@router_source.get("/{source_id}")
async def get_one_source(source_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> entities.Source:
    async with get_asession as session:
        source_repo = SourceRepoPostgres(session)
        source_service = CrudService(source_repo)
        source: entities.Source = await source_service.get_one({"id": source_id})
        return source


@router_source.get("/")
async def get_many_sources(get_asession=Depends(db.get_async_session)) -> list[entities.Source]:
    async with get_asession as session:
        source_repo = SourceRepoPostgres(session)
        source_service = CrudService(source_repo)
        sources: list[entities.Source] = await source_service.get_many({})
        return sources


@router_source.delete("/{source_id}")
async def delete_source(source_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> core_types.Id_:
    async with get_asession as session:
        source_repo = SourceRepoPostgres(session)
        source_service = CrudService(source_repo)
        deleted: entities.Source = await source_service.delete_one({"id": source_id})
        await session.commit()
        return deleted.id


@router_source.post("/{source_id}")
@helpers.async_timeit
async def bulk_append_wire_from_csv(source_id: core_types.Id_, file: UploadFile,
                                    get_asession=Depends(db.get_async_session)) -> int:
    df = pd.read_csv(file.file, parse_dates=['date'])
    df['date'] = pd.to_datetime(df['date'], utc=True)
    df['source_id'] = source_id
    event = events.WireManyCreated(source_id=source_id, wires=df.to_dict(orient='records'))
    async with get_asession as session:
        _ = await msgbus.handle(event, session)
        await session.commit()
        return 1


router_source_plan = APIRouter(
    prefix="/source-plan",
    tags=['SourcePlan']
)


@router_source_plan.post("/")
async def create_plan_item(data: events.PlanItemCreated,
                           get_asession=Depends(db.get_async_session)) -> entities.PlanItem:
    async with get_asession as session:
        result = await msgbus.handle(data, session)
        result: entities.PlanItem = result[events.PlanItemCreated]
        await session.commit()
        return result


@router_source_plan.get("/")
async def get_many_plan_items(source_id: core_types.Id_, sender: float = None, receiver: float = None,
                              sub1: str = None, sub2: str = None,
                              get_asession=Depends(db.get_async_session)) -> list[entities.PlanItem]:
    event = events.PlanItemListGotten(source_id=source_id, sender=sender, receiver=receiver, sub1=sub1, sub2=sub2)
    async with get_asession as session:
        result: dict = await msgbus.handle(event, session)
        result: list[entities.PlanItem] = result[events.PlanItemListGotten]
        await session.commit()
        return result


router_wire = APIRouter(
    prefix='/wire',
    tags=['Wire'],
)


@router_wire.post("/")
@helpers.async_timeit
async def create_one(data: events.WireCreated, get_asession=Depends(db.get_async_session)) -> entities.Wire:
    async with get_asession as session:
        results = await messagebus.handle(data, session)
        created: entities.Wire = results[0]
        await session.commit()
        return created


@router_wire.post("/many")
@helpers.async_timeit
async def create_many_wires(data: list[schema.WireCreateSchema], get_asession=Depends(db.get_async_session)) -> None:
    async with get_asession as session:
        wire_repo = WireRepoPostgres(session)
        wire_service = CrudService(wire_repo)
        await wire_service.create_many(data)
        await session.commit()


@router_wire.get("/{wire_id}")
@helpers.async_timeit
async def get_one(wire_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> entities.Wire:
    async with get_asession as session:
        wire_repo = WireRepoPostgres(session)
        wire_service = CrudService(wire_repo)
        filter_by = {"id": wire_id}
        wire: entities.Wire = await wire_service.get_one(filter_by)
        return wire


@router_wire.get("/")
@helpers.async_timeit
async def get_many(source_id: core_types.Id_,
                   date: datetime.datetime = None,
                   sender: float = None,
                   receiver: float = None,
                   debit: float = None,
                   credit: float = None,
                   subconto_first: str = None,
                   subconto_second: str = None,
                   comment: str = None,
                   paginate_from: int = None,
                   paginate_to: int = None,
                   get_asession=Depends(db.get_async_session)) -> list[entities.Wire]:
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
    async with get_asession as session:
        wire_repo = WireRepoPostgres(session)
        wire_service = CrudService(wire_repo)
        wires: list[entities.Wire] = await wire_service.get_many(filter_by,
                                                                 slice_from=paginate_from, slice_to=paginate_to)
        return wires


@router_wire.patch("/{wire_id}")
@helpers.async_timeit
async def partial_update_one(wire_id: core_types.Id_, data: events.WirePartialUpdated,
                             get_asession=Depends(db.get_async_session)) -> schema.WireSchema:
    data.wire_id = wire_id
    async with get_asession as session:
        result = await msgbus.handle(data, session)
        updated: entities.Wire = result[events.WirePartialUpdated]
        await session.commit()
        return updated


@router_wire.delete("/{wire_id}")
@helpers.async_timeit
async def delete_one(wire_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> int:
    async with get_asession as session:
        event = events.WireDeleted(wire_id=wire_id)
        results = await messagebus.handle(event, session)
        deleted_id = results[0]
        await session.commit()
        return deleted_id
