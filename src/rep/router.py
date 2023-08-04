from fastapi import APIRouter, Depends

from src import helpers, db, core_types
from . import entities, enums, events
from .messagebus import MessageBus

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
        result = await MessageBus(session).handle(event)
        report: entities.Report = result[0]
        await session.commit()
        return report


@router_report.get("/{report_id}")
@helpers.async_timeit
async def get_report(report_id: core_types.Id_, get_asession=Depends(db.get_async_session)) -> entities.Report:
    async with get_asession as session:
        event = events.ReportGotten(report_id=report_id)
        result = await MessageBus(session).handle(event)
        result = result.pop()
        await session.commit()
        return result


@router_report.get("/")
@helpers.async_timeit
async def get_reports(category: enums.ReportCategory = None,
                      get_asession=Depends(db.get_async_session)) -> list[entities.Report]:
    async with get_asession as session:
        event = events.ReportListGotten(category=get_inner_category(category))
        result = await MessageBus(session).handle(event)
        groups: list[entities.Report] = result[0]
        await session.commit()
        return groups