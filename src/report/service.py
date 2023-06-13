import pandas as pd
from loguru import logger
from pandera.typing import DataFrame
import finrep

from .. import core_types
from . import schema


class Service:
    wire_repo: repository.IWireRepository = repository.WireRepository
    sheet_repo: repository.ISheetRepository = repository.SheetRepository


class BalanceService(Service):
    interval: finrep.Interval = finrep.BalanceInterval
    group: finrep.Group = finrep.BalanceGroup
    report: finrep.Report = finrep.BalanceReport

    async def create_balance_group(self, data: schema.GroupCreate) -> core_types.Id_:
        wire_df: DataFrame[finrep.typing.WireSchema] = await self.wire_repo().retrieve_wire_df(data.wire_base_id)
        balance_group = self.group(wire_df=wire_df)
        balance_group.create_group(ccols=data.columns)
        balance_group = balance_group.get_group()
        sheet_id = await self.sheet_repo().create_sheet(balance_group, drop_index=True, drop_columns=False)
        logger.debug(f"\n{balance_group.to_string()}")
        return sheet_id

    async def create_balance_report(self, data: schema.ReportCreate) -> core_types.Id_:
        wire_df: DataFrame[finrep.typing.WireSchema] = await self.wire_repo().retrieve_wire_df(data.wire_base_id)
        group_df: pd.DataFrame = await self.sheet_repo().retrieve_sheet_df(data.group_id)

        interval = self.interval(**data.interval.dict())
        report = self.report(wire_df, group_df, interval)
        report.create_report()
        report = report.get_report()

        logger.warning(f"\n{report.to_string()}")
        return 123
