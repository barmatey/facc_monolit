from loguru import logger
import pandas as pd
from abc import ABC, abstractmethod
from pandera.typing import DataFrame

import finrep
from finrep.types import WireSchema

from .. import core_types
from . import entities, schema, enums
from .repository import Repository, PostgresRepo


class Service(ABC):

    @abstractmethod
    async def retrieve_report_categories(self) -> list[enums.CategoryLiteral]:
        pass

    @abstractmethod
    async def create_group(self, data: schema.GroupCreateSchema) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_group(self, id_: core_types.Id_) -> entities.Group:
        pass

    @abstractmethod
    async def retrieve_group_list(self, **kwargs) -> list[schema.GroupSchema]:
        pass

    @abstractmethod
    async def delete_group(self, id_: core_types.Id_) -> core_types.Id_:
        pass

    @abstractmethod
    async def create_report(self, data: schema.ReportCreateSchema) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_report(self, id_: core_types.Id_) -> entities.Report:
        pass

    @abstractmethod
    async def retrieve_report_list(self, **kwargs) -> list[entities.Report]:
        pass


class BaseService(Service):
    repo: Repository = PostgresRepo

    async def retrieve_report_categories(self) -> list[enums.CategoryLiteral]:
        return await self.repo().retrieve_report_categories()

    async def create_group(self, data: schema.GroupCreateSchema) -> core_types.Id_:
        raise NotImplemented

    async def create_report(self, data: schema.ReportCreateSchema) -> core_types.Id_:
        raise NotImplemented

    async def retrieve_group(self, id_: core_types.Id_) -> entities.Group:
        group: entities.Group = await self.repo().retrieve_group(id_)
        return group

    async def retrieve_group_list(self, **kwargs) -> list[entities.Group]:
        groups: list[entities.Group] = await self.repo().retrieve_group_list(**kwargs)
        return groups

    async def delete_group(self, id_: core_types.Id_) -> core_types.Id_:
        deleted_id = await self.repo().delete_group(id_)
        return deleted_id

    async def retrieve_report(self, id_: core_types.Id_) -> entities.Report:
        report: entities.Report = await self.repo().retrieve_report(id_)
        return report

    async def retrieve_report_list(self, **kwargs) -> list[entities.Report]:
        reports: list[entities.Report] = await self.repo().retrieve_report_list(**kwargs)
        return reports


class BalanceService(BaseService):
    group = finrep.BalanceGroup
    report = finrep.BalanceReport
    interval = finrep.BalanceInterval

    async def create_group(self, data: schema.GroupCreateSchema) -> core_types.Id_:
        # Get source base
        wire: DataFrame[WireSchema] = await self.repo().retrieve_wire_df(data.source_id)

        # Create group df
        balance = self.group(wire)
        balance.create_group(data.columns)
        balance_group: pd.DataFrame = balance.get_group()

        # Create group model
        group_create_data = entities.GroupCreate(
            title=data.title,
            source_id=data.source_id,
            columns=data.columns,
            dataframe=balance_group,
            drop_index=True,
            drop_columns=False,
            category='BALANCE',
        )
        group_id = await self.repo().create_group(group_create_data)
        return group_id

    async def create_report(self, data: schema.ReportCreateSchema) -> core_types.Id_:
        # Get source base
        wire_df: DataFrame[WireSchema] = await self.repo().retrieve_wire_df(data.source_id)

        # Get group df
        group_df: pd.DataFrame = await self.repo().retrieve_group_sheet_as_dataframe(data.group_id)

        # Create report df
        interval = self.interval(**data.interval.dict())
        report = self.report(wire_df, group_df, interval)
        report.create_report()
        report_df = report.get_report()

        # Create report model
        interval_create_data = entities.ReportIntervalCreate(
            start_date=data.interval.start_date,
            end_date=data.interval.end_date,
            period_year=data.interval.iyear,
            period_month=data.interval.imonth,
            period_day=data.interval.iday,
        )
        report_create_data = entities.ReportCreate(
            title=data.title,
            category='BALANCE',
            source_id=data.source_id,
            group_id=data.group_id,
            interval=interval_create_data,
            sheet=entities.SheetCreate(dataframe=report_df, drop_index=False, drop_columns=False)
        )
        report_id = await self.repo().create_report(report_create_data)
        return report_id
