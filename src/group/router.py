from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from loguru import logger

from src import helpers, db,  core_types
from src.repository_postgres_new import GroupRepoPostgres, WireRepoPostgres
from src.service_finrep import Finrep, BalanceFinrep, ProfitFinrep

from .entities import Group, ExpandedGroup
from .enums import GroupCategory
from . import events
from . import messagebus
from .service import GroupService

FINREP = {
    "BALANCE": BalanceFinrep,
    "PROFIT": ProfitFinrep,
    "CASHFLOW": ProfitFinrep,
}


def get_finrep(category: GroupCategory | None = None) -> Finrep:
    if category is None:
        category = "BALANCE"
    return FINREP[category]()


router_group = APIRouter(
    prefix="/group",
    tags=['Group']
)


@router_group.post("/")
@helpers.async_timeit
async def create_group(data: events.GroupCreated, get_asession=Depends(db.get_async_session)) -> Group:
    async with get_asession as session:
        results = await messagebus.handle(data, session)
        group: Group = results.pop()
        await session.commit()
        return group


@router_group.get("/{group_id}")
@helpers.async_timeit
async def get_group(group_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> JSONResponse:
    async with get_asession as session:
        event = events.GroupGotten(group_id=group_id)
        results = await messagebus.handle(event, session)
        group: ExpandedGroup = results[0]
        await session.commit()
        return JSONResponse(content=group.to_json())


@router_group.get("/")
@helpers.async_timeit
async def get_groups(category: GroupCategory = None,
                     get_asession=Depends(db.get_async_session)) -> list[Group]:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        group_service = GroupService(group_repo)
        groups: list[Group] = await group_service.get_many({})
        return groups


@router_group.patch("/{group_id}")
@helpers.async_timeit
async def partial_update_group(group_id: core_types.Id_, data: events.GroupPartialUpdated,
                               get_asession=Depends(db.get_async_session)) -> Group:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        group_service = GroupService(group_repo)
        updated: Group = await group_service.update_one(data, filter_by={"id": group_id})
        await session.commit()
        return updated

#
# @router_group.patch("/{group_id}/total-recalculate")
# @helpers.async_timeit
# async def total_recalculate(group_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> entities.Group:
#     async with get_asession as session:
#         group_repo = GroupRepoPostgres(session)
#         wire_repo = WireRepoPostgres(session)
#         group_service = GroupService(group_repo, wire_repo)
#         instance = await group_service.get_one({"id": group_id})
#         updated: entities.Group = await group_service.total_recalculate(instance)
#         await session.commit()
#         return updated
#
#
# @router_group.delete("/{group_id}")
# @helpers.async_timeit
# async def delete_group(group_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> core_types.Id_:
#     async with get_asession as session:
#         group_repo = GroupRepoPostgres(session)
#         wire_repo = WireRepoPostgres(session)
#         group_service = GroupService(group_repo, wire_repo)
#         deleted_id = await group_service.delete_one({"id": group_id})
#         await session.commit()
#         return deleted_id
