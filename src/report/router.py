from fastapi import APIRouter, Depends

from .. import core_types
from ..report.entities import GroupCreate, ReportCreate, ReportIntervalCreate
from ..report.service import Service, BalanceService

router_report = APIRouter(
    prefix="/report",
    tags=['Report']
)


@router_report.post("/create-balance-group")
async def create_balance_group(group_data: GroupCreate, service: Service = Depends(BalanceService)) -> core_types.Id_:
    return await service.create_group(group_data)


@router_report.post("/create-balance-report")
async def create_balance_report(
        report_data: ReportCreate,
        interval_data: ReportIntervalCreate,
        service: Service = Depends(BalanceService),
) -> core_types.Id_:
    return 1
