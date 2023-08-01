import enum

from fastapi import APIRouter, Depends
from loguru import logger

from src.repository_postgres_new.report import ReportRepoPostgres
from src import helpers, db
from src.repository_postgres_new import GroupRepoPostgres, WireRepoPostgres, CategoryRepoPostgres
from .. import core_types
from .service_crud import Service, ReportService, GroupService
from . import schema, enums, entities


class LinkedCategory(enum.Enum):
    BALANCE = 1
    PROFIT = 2
    CASHFLOW = 3


def get_category_id(category: enums.CategoryLiteral | None) -> int | None:
    result = LinkedCategory[category].value if category is not None else None
    return result


router_group = APIRouter(
    prefix="/group",
    tags=['Group']
)


@router_group.post("/")
@helpers.async_timeit
async def create_group(data: schema.GroupCreateSchema, get_asession=Depends(db.get_async_session)) -> entities.Group:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        group_service = GroupService(group_repo, wire_repo)
        group = await group_service.create_one(data)
        await session.commit()
        return group


@router_group.get("/{group_id}")
@helpers.async_timeit
async def get_group(group_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> entities.Group:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        group_service = GroupService(group_repo, wire_repo)
        group: entities.Group = await group_service.get_one({"id": group_id})
        return group


@router_group.get("/")
@helpers.async_timeit
async def get_groups(category: enums.CategoryLiteral = None,
                     get_asession=Depends(db.get_async_session)) -> list[entities.Group]:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        group_service = GroupService(group_repo, wire_repo)
        groups: list[entities.Group] = await group_service.get_many({})
        return groups


@router_group.patch("/{group_id}")
@helpers.async_timeit
async def partial_update_group(group_id: core_types.Id_, data: schema.GroupPartialUpdateSchema,
                               get_asession=Depends(db.get_async_session)) -> entities.Group:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        group_service = GroupService(group_repo, wire_repo)
        updated: entities.Group = await group_service.update_one(data, filter_by={"id": group_id})
        await session.commit()
        return updated


@router_group.patch("/{group_id}/total-recalculate")
@helpers.async_timeit
async def total_recalculate(group_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> entities.Group:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        group_service = GroupService(group_repo, wire_repo)
        instance = await group_service.get_one({"id": group_id})
        updated: entities.Group = await group_service.total_recalculate(instance)
        await session.commit()
        return updated


@router_group.delete("/{group_id}")
@helpers.async_timeit
async def delete_group(group_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> core_types.Id_:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        group_service = GroupService(group_repo, wire_repo)
        deleted_id = await group_service.delete_one({"id": group_id})
        await session.commit()
        return deleted_id


"""
REPORT
"""

router_report = APIRouter(
    prefix="/report",
    tags=['Report'],
)


@router_report.post("/")
@helpers.async_timeit
async def create_report(data: schema.ReportCreateSchema, get_asession=Depends(db.get_async_session)) -> entities.Report:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        report_repo = ReportRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        report_service = ReportService(report_repo, wire_repo, group_repo)
        report: entities.Report = await report_service.create_one(data)
        await session.commit()
        return report


@router_report.get("/{report_id}")
@helpers.async_timeit
async def retrieve_report(report_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> entities.Report:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        report_repo = ReportRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        report_service = ReportService(report_repo, wire_repo, group_repo)
        filter_by = {"id": report_id}
        report: entities.Report = await report_service.get_one(filter_by)
        return report


@router_report.get("/")
@helpers.async_timeit
async def retrieve_report_list(category: enums.CategoryLiteral = None,
                               get_asession=Depends(db.get_async_session)) -> list[entities.Report]:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        report_repo = ReportRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        report_service = ReportService(report_repo, wire_repo, group_repo)
        filter_by = {"category_id": get_category_id(category)}
        reports: list[entities.Report] = await report_service.get_many(filter_by)
        return reports


@router_report.delete("/{report_id}")
@helpers.async_timeit
async def delete_report(report_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> core_types.Id_:
    async with get_asession as session:
        group_repo = GroupRepoPostgres(session)
        report_repo = ReportRepoPostgres(session)
        wire_repo = WireRepoPostgres(session)
        report_service = ReportService(report_repo, wire_repo, group_repo)
        deleted_id = await report_service.delete_one({"id": report_id})
        await session.commit()
        return deleted_id


# @router_report.patch("/{report_id}")
# @helpers.async_timeit
# async def total_recalculate(report_id: core_types.Id_,
#                             service: ReportService = Depends(ReportService)) -> entities.Report:
#     report = await service.retrieve({"id": report_id})
#     updated = await service.total_recalculate(report)
#     return updated


"""
CATEGORY
"""

router_category = APIRouter(
    prefix="/category",
    tags=['Category'],
)


@router_category.post("/")
async def create(data: schema.ReportCategoryCreateSchema,
                 get_asession=Depends(db.get_async_session)) -> entities.ReportCategory:
    async with get_asession as session:
        category_repo = CategoryRepoPostgres(session)
        category_service = Service(category_repo)
        category: entities.ReportCategory = await category_service.create_one(data)
        await session.commit()
        return category


@router_category.get("/")
async def retrieve_report_categories(get_asession=Depends(db.get_async_session)) -> list[schema.ReportCategorySchema]:
    async with get_asession as session:
        category_repo = CategoryRepoPostgres(session)
        category_service = Service(category_repo)
        result: list[entities.ReportCategory] = await category_service.get_many({})
        categories = [category.value for category in result]
        return categories
