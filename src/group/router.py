from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from src import helpers, db, core_types
from src import messagebus
from .entities import Group, ExpandedGroup
from .enums import GroupCategory
from . import events


router_group = APIRouter(
    prefix="/group",
    tags=['Group']
)


@router_group.post("/")
@helpers.async_timeit
async def create_group(data: events.GroupCreated, get_asession=Depends(db.get_async_session)) -> Group:
    async with get_asession as session:
        group: Group = await messagebus.handle(data, session)
        await session.commit()
        return group


@router_group.get("/{group_id}")
@helpers.async_timeit
async def get_group(group_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> JSONResponse:
    async with get_asession as session:
        event = events.GroupGotten(group_id=group_id)
        group: ExpandedGroup = await messagebus.handle(event, session)
        await session.commit()
        return JSONResponse(content=group.to_json())


@router_group.get("/")
@helpers.async_timeit
async def get_groups(category: GroupCategory = None,
                     get_asession=Depends(db.get_async_session)) -> list[Group]:
    async with get_asession as session:
        event = events.GroupListGotten()
        groups: list[Group] = await messagebus.handle(event, session)
        await session.commit()
        return groups


@router_group.patch("/{group_id}")
@helpers.async_timeit
async def partial_update_group(group_id: core_types.Id_, data: events.GroupPartialUpdated,
                               get_asession=Depends(db.get_async_session)) -> Group:
    async with get_asession as session:
        event = data.copy()
        event.id = group_id
        updated: Group = await messagebus.handle(data, session)
        await session.commit()
        return updated


@router_group.delete("/{group_id}")
@helpers.async_timeit
async def delete_group(group_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> core_types.Id_:
    async with get_asession as session:
        event = events.GroupDeleted(group_id=group_id)
        deleted_id: core_types.Id_ = await messagebus.handle(event, session)
        await session.commit()
        return deleted_id
