from fastapi import APIRouter, Depends

from src import helpers, db
from src.repository_postgres_new import GroupRepoPostgres, WireRepoPostgres
from src.service_finrep import Finrep, BalanceFinrep, ProfitFinrep

from .entities import Group
from .enums import GroupCategory
from .events import CreateGroupRequest
from .service import ServiceGroup

FINREP = {
    "BALANCE": BalanceFinrep,
    "PROFIT": ProfitFinrep,
    "CASHFLOW": ProfitFinrep,
}


def get_finrep(category: GroupCategory) -> Finrep:
    return FINREP[category]()


router_group = APIRouter(
    prefix="/group",
    tags=['Group']
)


@router_group.post("/")
@helpers.async_timeit
async def create_group(data: CreateGroupRequest, get_asession=Depends(db.get_async_session)) -> Group:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        group_service = ServiceGroup(group_repo, wire_repo, get_finrep(data.category))
        group = await group_service.create_one(data)
        await session.commit()
        return group

#
# @router_group.get("/{group_id}")
# @helpers.async_timeit
# async def get_group(group_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> entities.Group:
#     async with get_asession as session:
#         group_repo = GroupRepoPostgres(session)
#         wire_repo = WireRepoPostgres(session)
#         group_service = GroupService(group_repo, wire_repo)
#         group: entities.Group = await group_service.get_one({"id": group_id})
#         return group
#
#
# @router_group.get("/")
# @helpers.async_timeit
# async def get_groups(category: enums.CategoryLiteral = None,
#                      get_asession=Depends(db.get_async_session)) -> list[entities.Group]:
#     async with get_asession as session:
#         group_repo = GroupRepoPostgres(session)
#         wire_repo = WireRepoPostgres(session)
#         group_service = GroupService(group_repo, wire_repo)
#         groups: list[entities.Group] = await group_service.get_many({})
#         return groups
#
#
# @router_group.patch("/{group_id}")
# @helpers.async_timeit
# async def partial_update_group(group_id: core_types.Id_, data: schema.GroupPartialUpdateSchema,
#                                get_asession=Depends(db.get_async_session)) -> entities.Group:
#     async with get_asession as session:
#         group_repo = GroupRepoPostgres(session)
#         wire_repo = WireRepoPostgres(session)
#         group_service = GroupService(group_repo, wire_repo)
#         updated: entities.Group = await group_service.update_one(data, filter_by={"id": group_id})
#         await session.commit()
#         return updated
#
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
