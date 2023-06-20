from fastapi import APIRouter, Depends

from .. import core_types
from ..repository_postgres.report import ReportRepo
from ..report.entities import ReportCreate, ReportIntervalCreate
from ..report.service import Service, BalanceService
from ..report import schema
from ..report import enums

router_report = APIRouter(
    prefix="/report",
    tags=['Report']
)


@router_report.post("/")
async def create_report(
        report_data: ReportCreate,
        interval_data: ReportIntervalCreate,
        repo: ReportRepo = Depends(ReportRepo)
) -> core_types.Id_:
    id_ = await repo.create(report_data, interval_data)
    return id_


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
#
