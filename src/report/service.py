import pandas as pd
from loguru import logger
from pandera.typing import DataFrame
import finrep

from .. import core_types
from ..database.sheet import repository_sheet
from ..database.report import repository_report
from . import schema, schema_output
from . import repository


class GroupService:
    repo = repository.GroupRepo

    async def create_group(self, data: schema.GroupCreateForm) -> core_types.Id_:
        return await self.repo().create(data)

    async def retrieve_group(self, data: schema.GroupRetrieveForm) -> schema_output.Group:
        return await self.repo().retrieve(data)

    async def delete_group(self, data: schema.GroupDeleteForm) -> None:
        await self.repo().delete(data)

    async def retrieve_group_list(self) -> list[schema_output.Group]:
        pass


class ReportService:
    repo: repository.ReportRepo

    async def create_report(self):
        pass

    async def retrieve_report(self):
        pass

    async def delete_report(self):
        pass

    async def retrieve_report_list(self):
        pass


class Service:
    sheet_repo: repository_sheet.SheetCrudRepo = repository_sheet.SheetCrudRepo
    report_repo: repository_report.ReportRepo = repository_report.ReportRepoPostgres


# class BalanceService(Service):
#     interval: finrep.Interval = finrep.BalanceInterval
#     group: finrep.Group = finrep.BalanceGroup
#     report: finrep.Report = finrep.BalanceReport
#
#     async def create_balance_group(self, data: schema.GroupCreateForm) -> core_types.Id_:
#         # Get source wires
#         wire_df: DataFrame[finrep.typing.WireSchema] = await self.wire_repo().retrieve_wire_df(data.wire_base_id)
#
#         # Create group dataframe
#         balance_group_sheet = self.group(wire_df=wire_df)
#         balance_group_sheet.create_group(ccols=data.columns)
#         balance_group_sheet = balance_group_sheet.get_group()
#
#         # Create sheet model
#         _sheet_id = await self.sheet_repo().create_sheet(balance_group_sheet, drop_index=True, drop_columns=False)
#
#         # Create group model
#         group_id = -1
#
#         return group_id
#
#     async def create_balance_report(self, data: schema.ReportCreateForm) -> core_types.Id_:
#         # Get source wires and group dataframe
#         wire_df: DataFrame[finrep.typing.WireSchema] = await self.wire_repo().retrieve_wire_df(data.wire_base_id)
#         group_df: pd.DataFrame = await self.sheet_repo().retrieve_sheet(data.group_id)
#
#         # Create report dataframe
#         interval = self.interval(**data.interval.dict())
#         report = self.report(wire_df, group_df, interval)
#         report.create_report()
#         report = report.get_report()
#
#         # Create sheet model
#         _sheet_id = await self.sheet_repo().create_sheet(report, drop_index=False, drop_columns=False)
#
#         # Create report model
#
#         logger.warning(f"\n{report.to_string()}")
#         return 123
