import typing

from pydantic import BaseModel

from src import core_types
from . import entities, enums

SheetRetrieveSchema = entities.SheetRetrieve
SheetSchema = entities.Sheet
SheetCreateSchema = entities.SheetCreate


class ScrollSizeSchema(entities.ScrollSize):
    @classmethod
    def from_scroll_size_entity(cls, data: entities.ScrollSize):
        return cls(
            count_cols=data.count_cols,
            count_rows=data.count_rows,
            scroll_width=data.scroll_width,
            scroll_height=data.scroll_height,
        )


ColFilterRetrieveSchema = entities.ColFilterRetrieve
ColFilterSchema = entities.ColFilter
ColSorterSchema = entities.ColSorter
CopySindexSchema = entities.CopySindex
CellSchema = entities.Cell
CopyCellSchema = entities.CopyCell

UpdateSindexSizeSchema = entities.UpdateSindexSize
UpdateCellSchema = entities.UpdateCell


class PartialUpdateCellSchema(BaseModel):
    id: core_types.Id_
    sheet_id: core_types.Id_
    value: typing.Optional[str]
    dtype: typing.Optional[enums.Dtype]
    is_readonly: typing.Optional[bool]
    is_filtred: typing.Optional[bool]
    is_index: typing.Optional[bool]
    text_align: typing.Optional[enums.CellTextAlign]
    color: typing.Optional[str]
