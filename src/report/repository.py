from abc import ABC, abstractmethod
import pandas as pd

from . import entities
from .. import core_types


class Repository(ABC):

    @abstractmethod
    async def retrieve_wire_df(self, source_id: core_types.Id_) -> pd.DataFrame:
        pass

    @abstractmethod
    async def create_group(self, data: entities.GroupCreate) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_group(self, group_id: core_types.Id_) -> entities.GroupRetrieve:
        pass

    @abstractmethod
    async def retrieve_group_list(self) -> list[entities.GroupRetrieve]:
        pass

    @abstractmethod
    async def delete_group(self, group_id: core_types.Id_) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_group_sheet_as_dataframe(self, group_id: core_types.Id_) -> pd.DataFrame:
        pass

    @abstractmethod
    async def create_report(self, data: entities.ReportCreate) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_report(self, report_id: core_types.Id_) -> entities.ReportRetrieve:
        pass

    @abstractmethod
    async def retrieve_report_list(self) -> list[entities.ReportRetrieve]:
        pass

    @abstractmethod
    async def delete_report(self, report_id: core_types.Id_) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_report_sheet_as_dataframe(self, report_id: core_types.Id_) -> pd.DataFrame:
        pass
