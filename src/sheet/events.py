import typing

import pandas as pd

from src import core_types
from src.core_types import Event

from . import entities, enums, schema


class SheetCreated(Event):
    df: pd.DataFrame
    drop_index: bool
    drop_columns: bool
    readonly_all_cells: bool = False


class SheetGotten(Event):
    sheet_id: core_types.Id_
    from_scroll: typing.Optional[int] = None
    to_scroll: typing.Optional[int] = None


class ColFilterGotten(Event):
    sheet_id: core_types.Id_
    col_id: core_types.Id_


class ColFilterUpdated(Event):
    sheet_id: core_types.Id_
    col_filter: entities.ColFilter


class ColFiltersDropped(Event):
    sheet_id: core_types.Id_


class ColSortedUpdated(Event):
    sheet_id: core_types.Id_
    col_sorter: entities.ColSorter


class ColWidthUpdated(Event):
    sindex_id: core_types.Id_
    new_size: int
    sheet_id: typing.Optional[core_types.Id_] = None


class CellsPartialUpdated(Event):
    sheet_id: core_types.Id_
    cells: list[schema.PartialUpdateCellSchema]


class RowsDeleted(Event):
    sheet_id: core_types.Id_
    row_ids: list[core_types.Id_]


class SheetInfoUpdated(Event):
    sheet_id: core_types.Id_
    data: dict
