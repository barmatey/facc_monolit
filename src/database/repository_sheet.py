from abc import abstractmethod, ABC

from . import schema_input, schema_output
from .. import core_types


class SheetCrudRepo(ABC):

    @abstractmethod
    async def create_sheet(self, data: schema_input.Sheet) -> core_types.Id_:
        pass

    @abstractmethod
    async def retrieve_sheet(self, data: schema_input.RetrieveSheetForm) -> schema_output.Sheet:
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
    async def copy_rows(self, data: input_schema.CopySindexesData) -> None:
        pass

    @abstractmethod
    async def copy_cols(self, data: input_schema.CopySindexesData) -> None:
        pass

    @abstractmethod
    async def copy_cells(self, data: input_schema.CopyCellsData) -> None:
        pass

    @abstractmethod
    async def update_cells(self, data: input_schema.UpdateCellsData) -> None:
        pass

    @abstractmethod
    async def delete_rows(self, data: input_schema.DeleteSindexesData) -> None:
        pass

    @abstractmethod
    async def update_col_width(self, data: input_schema.UpdateColWidthData) -> output_schema.Sindex:
        pass


class SheetSupportRepo(ABC):
    @abstractmethod
    async def get_sheet_scroll_size(self, sheet_id: core_types.Id_) -> output_schema.ScrollSize:
        pass


class SheetCrudRepoPostgres(SheetCrudRepo):
    pass


class SheetFilterRepoPostgres(SheetFilterRepo):
    pass


class SheetTableUpdateRepoPostgres(SheetTableUpdateRepo):
    pass


class SheetSupportRepoPostgres(SheetSupportRepo):
    pass
