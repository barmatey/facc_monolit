from fastapi import APIRouter, Depends

from .. import core_types
from ..repository.group import GroupRepo
from ..report import entities
from ..report import schema
from ..report import enums

router_group = APIRouter(
    prefix="/group",
    tags=['Group']
)


@router_group.post("/")
async def create_group(data: schema.GroupCreateSchema, repo: GroupRepo = Depends(GroupRepo)) -> core_types.Id_:
    data = entities.GroupCreate(
        title=data.title,
        category_id=enums.Category[data.category].value,
        source_id=data.source_id,
    )
    id_ = await repo.create(data)
    return id_


@router_group.get("/{id_}")
async def retrieve_group(id_: core_types.Id_, repo: GroupRepo = Depends(GroupRepo)) -> schema.GroupRetrieveSchema:
    group = await repo.retrieve(id_)
    group = schema.GroupRetrieveSchema(
        id=group.id,
        title=group.title,
        category=enums.Category(group.category_id).name,
        source_id=group.source_id,
    )
    return group


@router_group.delete("/{id_}")
async def delete_group(id_: core_types.Id_, repo: GroupRepo = Depends(GroupRepo)) -> int:
    await repo.delete(id_)
    return 1
