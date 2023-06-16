from fastapi import APIRouter, Depends

from .. import core_types
from ..repository.group import GroupRepo
from ..repository.report import ReportRepo
from . import entities
from . import schema
from . import enums

# router_report = APIRouter(
#     prefix="/report",
#     tags=['Report']
# )


# @router_report.post("/")
# async def create_report(report_data: entities.ReportCreate, interval_data: entities.ReportIntervalCreate,
#                         repo: ReportRepo = Depends(ReportRepo)) -> core_types.Id_:
#     id_ = await repo.create(report_data, interval_data)
#     return id_


#
# @router_report.get("/{id_}")
# async def retrieve_report(data: schema.ReportRetrieveForm, repo=ReportRepo) -> schema_output.Report:
#     pass
#
#
# @router_report.delete("/{id_}")
# async def delete_report():
#     pass
#
#
# @router_report.get("/")
# async def retrieve_report_list():
#     pass


router_group = APIRouter(
    prefix="/group",
    tags=['Group']
)


@router_group.post("/")
async def create_group(data: schema.GroupCreateForm, repo: GroupRepo = Depends(GroupRepo)) -> core_types.Id_:
    data = entities.GroupCreate(
        title=data.title,
        category_id=enums.Category[data.category].value,
        sheet_id='',
        source_id=data.source_id,
    )
    id_ = await repo.create(data)
    return id_


@router_group.get("/{id_}")
async def retrieve_group(id_: core_types.Id_, repo: GroupRepo = Depends(GroupRepo)) -> schema.GroupResponse:
    group = await repo.retrieve(id_)
    group = schema.GroupResponse(
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
