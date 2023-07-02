from abc import ABC, abstractmethod
import pandas as pd

import core_types
import repository_postgres
from . import entities, enums


class Repository(ABC):

    @abstractmethod
    async def retrieve_wire_df(self, source_id: core_types.Id_) -> pd.DataFrame:
        pass

    @abstractmethod
    async def create_group(self, data: entities.GroupCreate) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_group(self, group_id: core_types.Id_) -> entities.Group:
        pass

    @abstractmethod
    async def retrieve_group_list(self) -> list[entities.Group]:
        pass

    @abstractmethod
    async def delete_group(self, group_id: core_types.Id_) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_group_sheet_as_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        pass

    @abstractmethod
    async def create_report(self, data: entities.ReportCreate) -> entities.Report:
        pass

    @abstractmethod
    async def retrieve_report(self, report_id: core_types.Id_) -> entities.Report:
        pass

    @abstractmethod
    async def retrieve_report_list(self) -> list[entities.Report]:
        pass

    @abstractmethod
    async def delete_report(self, report_id: core_types.Id_) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_report_sheet_as_dataframe(self, report_id: core_types.Id_) -> pd.DataFrame:
        pass

    @abstractmethod
    async def retrieve_report_categories(self) -> list[enums.CategoryLiteral]:
        pass


class PostgresRepo(Repository):
    wire_repo = repository_postgres.WireRepo
    group_repo = repository_postgres.GroupRepo
    report_repo = repository_postgres.ReportRepo
    category_repo = repository_postgres.CategoryRepo

    async def retrieve_wire_df(self, source_id: core_types.Id_) -> pd.DataFrame:
        return await self.wire_repo().retrieve_wire_df(source_id=source_id)

    async def create_group(self, data: entities.GroupCreate) -> entities.Group:
        return await self.group_repo().create(data)

    async def retrieve_group(self, group_id: core_types.Id_) -> entities.Group:
        return await self.group_repo().retrieve_by_id(id_=group_id)

    async def retrieve_group_list(self, **kwargs) -> list[entities.Group]:
        filter_ = {key: value for key, value in kwargs.items() if value is not None}
        return await self.group_repo().retrieve_bulk(filter_=filter_)

    async def delete_group(self, group_id: core_types.Id_) -> core_types.Id_:
        return await self.group_repo().delete_by_id(id_=group_id)

    async def retrieve_group_sheet_as_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        return await self.group_repo().retrieve_linked_sheet_as_dataframe(group_id=group_id)

    async def create_report(self, data: entities.ReportCreate) -> entities.Report:
        return await self.report_repo().create(data)

    async def retrieve_report(self, report_id: core_types.Id_) -> entities.Report:
        return await self.report_repo().retrieve_by_id(id_=report_id)

    async def retrieve_report_list(self, **kwargs) -> list[entities.Report]:
        filter_ = {key: value for key, value in kwargs.items() if value is not None}
        return await self.report_repo().retrieve_bulk(filter_=filter_)

    async def delete_report(self, report_id: core_types.Id_) -> core_types.Id_:
        return await self.report_repo().delete_by_id(report_id)

    async def retrieve_report_sheet_as_dataframe(self, report_id: core_types.Id_) -> pd.DataFrame:
        raise NotImplemented

    async def retrieve_report_categories(self) -> list[enums.CategoryLiteral]:
        return await self.category_repo().retrieve_bulk()
