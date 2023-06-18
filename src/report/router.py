from fastapi import APIRouter, Depends

from .. import core_types
from .service import Service, BalanceService
from . import schema

router_report = APIRouter(
    prefix="/report",
    tags=['Report']
)


@router_report.post("/create-balance-group")
async def create_balance_group(
        group_data: schema.GroupCreateSchema,
        service: Service = Depends(BalanceService)
) -> core_types.Id_:
    return await service.create_group(group_data)


@router_report.delete("/delete-group")
async def delete_group(id_: core_types.Id_, service: Service = Depends(BalanceService)) -> int:
    await service.delete_group(id_)
    return 1

# @router_report.post("/create-balance-report")
# async def create_balance_report(
#         report_data: ReportCreate,
#         interval_data: ReportIntervalCreate,
#         service: Service = Depends(BalanceService),
# ) -> core_types.Id_:
#     return 1
