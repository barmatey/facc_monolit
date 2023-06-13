import pandas as pd
from loguru import logger
from pandera.typing import DataFrame
import finrep

from .. import core_types
from ..database import repository_wire, repository_sheet, repository_report
from . import schema


class Service:
    wire_repo: repository_wire.WireRepo = repository_wire.WireRepoPostgres
    sheet_repo: repository_sheet.SheetCrudRepo = repository_sheet.SheetCrudRepoPostgres
    report_repo: repository_report.ReportRepo = repository_report.ReportRepoPostgres


class BalanceService(Service):
    interval: finrep.Interval = finrep.BalanceInterval
    group: finrep.Group = finrep.BalanceGroup
    report: finrep.Report = finrep.BalanceReport

    async def create_balance_group(self, data: schema.GroupCreate) -> core_types.Id_:
        # Get source wires
        wire_df: DataFrame[finrep.typing.WireSchema] = await self.wire_repo().retrieve_wire_df(data.wire_base_id)

        # Create group dataframe
        balance_group_sheet = self.group(wire_df=wire_df)
        balance_group_sheet.create_group(ccols=data.columns)
        balance_group_sheet = balance_group_sheet.get_group()

        # Create sheet model
        sheet_id = await self.sheet_repo().create_sheet(balance_group_sheet, drop_index=True, drop_columns=False)

        # Create group model


        return group_id

    async def create_balance_report(self, data: schema.ReportCreate) -> core_types.Id_:
        wire_df: DataFrame[finrep.typing.WireSchema] = await self.wire_repo().retrieve_wire_df(data.wire_base_id)
        group_df: pd.DataFrame = await self.sheet_repo().retrieve_sheet_df(data.group_id)

        interval = self.interval(**data.interval.dict())
        report = self.report(wire_df, group_df, interval)
        report.create_report()
        report = report.get_report()

        logger.warning(f"\n{report.to_string()}")
        return 123
