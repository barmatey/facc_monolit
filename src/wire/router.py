import pandas as pd
from fastapi import APIRouter, UploadFile
from loguru import logger

import db
from src import core_types, helpers
from src.repository_postgres_new import SourceRepoPostgres, WireRepoPostgres
from . import schema
from . import entities
from .service import Service

router_source = APIRouter(
    prefix="/source-db",
    tags=['SourceDB']
)


@router_source.post("/")
async def create_source(data: schema.SourceCreateSchema) -> entities.Source:
    async with db.get_async_session() as session:
        source_repo = SourceRepoPostgres(session)
        source_service = Service(source_repo)
        source: entities.Source = await source_service.create_one(data)
        await session.commit()
        return source


@router_source.get("/{source_id}")
async def retrieve_source(source_id: core_types.Id_) -> entities.Source:
    async with db.get_async_session() as session:
        source_repo = SourceRepoPostgres(session)
        source_service = Service(source_repo)
        source: entities.Source = await source_service.get_one({"id": source_id})
        return source


@router_source.delete("/{source_id}")
async def delete_source(source_id: core_types.Id_) -> core_types.Id_:
    async with db.get_async_session() as session:
        source_repo = SourceRepoPostgres(session)
        source_service = Service(source_repo)
        deleted_id: core_types.Id_ = await source_service.delete_one({"id": source_id})
        await session.commit()
        return deleted_id


@router_source.get("/")
async def list_source() -> list[entities.Source]:
    async with db.get_async_session() as session:
        source_repo = SourceRepoPostgres(session)
        source_service = Service(source_repo)
        sources: list[entities.Source] = await source_service.get_many({})
        return sources


@router_source.post("/{source_id}")
@helpers.async_timeit
async def bulk_append_wire_from_csv(source_id: core_types.Id_, file: UploadFile, ) -> int:
    df = pd.read_csv(file.file, parse_dates=['date'])
    df['date'] = pd.to_datetime(df['date'], utc=True)
    async with db.get_async_session() as session:
        wire_repo = WireRepoPostgres(session)
        _source_service = Service(wire_repo)
        raise NotImplemented


router_wire = APIRouter(
    prefix='/wire',
    tags=['Wire'],
)


@router_wire.post("/")
@helpers.async_timeit
async def create(data: schema.WireCreateSchema) -> entities.Wire:
    async with db.get_async_session() as session:
        wire_repo = SourceRepoPostgres(session)
        wire_service = Service(wire_repo)
        created: entities.Wire = await wire_service.create_one(data)
        await session.commit()
        return created


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
                        ) -> list[entities.Wire]:
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
    async with db.get_async_session() as session:
        wire_repo = WireRepoPostgres(session)
        wire_service = Service(wire_repo)
        wires: list[entities.Wire] = await wire_service.get_many(filter_by,
                                                                 slice_from=paginate_from, slice_to=paginate_to)
        return wires


@router_wire.patch("/{wire_id}")
@helpers.async_timeit
async def update(wire_id: core_types.Id_, data: schema.WireCreateSchema) -> schema.WireSchema:
    filter_by = {"id": wire_id}
    async with db.get_async_session() as session:
        wire_repo = WireRepoPostgres(session)
        wire_service = Service(wire_repo)
        updated = await wire_service.update_one(data, filter_by)
        await session.commit()
        return updated


@router_wire.delete("/{wire_id}")
@helpers.async_timeit
async def delete(wire_id: core_types.Id_) -> int:
    filter_by = {"id": wire_id}
    async with db.get_async_session() as session:
        wire_repo = WireRepoPostgres(session)
        wire_service = Service(wire_repo)
        deleted_id = await wire_service.delete_one(filter_by)
        await session.commit()
        return deleted_id
