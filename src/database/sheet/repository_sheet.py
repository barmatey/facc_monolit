from abc import abstractmethod, ABC

import pandas as pd

from . import schema_input, schema_output
from src import core_types


class SheetCrudRepo(ABC):

    @abstractmethod
    async def create_sheet(self, data: pd.DataFrame, drop_index: bool, drop_columns: bool) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_sheet(self, data: schema_input.RetrieveSheetForm) -> schema_output.Sheet:
        pass

    @abstractmethod
    async def retrieve_sheet_df(self, sheet_id: core_types.Id_) -> pd.DataFrame:
        pass

    @abstractmethod
    async def delete_sheet(self, sheet_id: core_types.Id_) -> None:
        pass


class SheetFilterRepo(ABC):

    @abstractmethod
    async def sort_sheet(self, data: schema_input.SortSheetForm):
        pass

    @abstractmethod
    async def retrieve_filter_items(
            self, sheet_id: core_types.Id_, col_id: core_types.Id_) -> list[schema_output.FilterItem]:
        pass

    @abstractmethod
    async def update_col_filter(
            self, sheet_id: core_types.Id_, data: schema_input.UpdateColFilterForm) -> list[schema_output.FilterItem]:
        pass


class SheetTableUpdateRepo(ABC):

    @abstractmethod
    async def copy_rows(self, data: schema_input.CopySindexesForm) -> None:
        pass

    @abstractmethod
    async def copy_cols(self, data: schema_input.CopySindexesForm) -> None:
        pass

    @abstractmethod
    async def copy_cells(self, data: schema_input.CopyCellsForm) -> None:
        pass

    @abstractmethod
    async def update_cells(self, data: schema_input.UpdateCellsForm) -> None:
        pass

    @abstractmethod
    async def delete_rows(self, data: schema_input.DeleteSindexesForm) -> None:
        pass

    @abstractmethod
    async def update_col_width(self, data: schema_input.UpdateColWidthForm) -> None:
        pass


class SheetSupportRepo(ABC):
    @abstractmethod
    async def get_sheet_scroll_size(self, sheet_id: core_types.Id_) -> schema_output.ScrollSize:
        pass
