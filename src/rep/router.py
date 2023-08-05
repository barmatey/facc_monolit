from fastapi import APIRouter, Depends

from src import helpers, db, core_types
from src import messagebus
from . import entities, enums, events

router_report = APIRouter(
    prefix="/report",
    tags=['Report'],
)


def get_inner_category(value: enums.ReportCategory) -> entities.InnerCategory:
    categories = {
        "BALANCE": entities.InnerCategory(id=1, value="BALANCE"),
        "PROFIT": entities.InnerCategory(id=2, value="PROFIT"),
        "CASHFLOW": entities.InnerCategory(id=3, value="CASHFLOW")
    }
    return categories[value]


@router_report.post("/")
@helpers.async_timeit
async def create_report(event: events.ReportCreated, get_asession=Depends(db.get_async_session)) -> entities.Report:
    async with get_asession as session:
        result = await messagebus.handle(event, session)
        report: entities.Report = result[events.ReportGotten]
        await session.commit()
        return report


@router_report.get("/{report_id}")
@helpers.async_timeit
async def get_report(report_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> entities.Report:
    async with get_asession as session:
        event = events.ReportGotten(report_id=report_id)
        result = await messagebus.handle(event, session)
        report = result[events.ReportGotten]
        await session.commit()
        return report


@router_report.get("/")
@helpers.async_timeit
async def get_reports(category: enums.ReportCategory = None,
                      get_asession=Depends(db.get_async_session)) -> list[entities.Report]:
    async with get_asession as session:
        event = events.ReportListGotten(category=get_inner_category(category))
        result = await messagebus.handle(event, session)
        groups: list[entities.Report] = result[events.ReportListGotten]
        await session.commit()
        return groups


@router_report.delete("/{report_id}")
@helpers.async_timeit
async def delete_report(report_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> core_types.Id_:
    async with get_asession as session:
        event = events.ReportDeleted(report_id=report_id)
        result = await messagebus.handle(event, session)
        deleted_id = result[events.ReportDeleted]
        await session.commit()
        return deleted_id
