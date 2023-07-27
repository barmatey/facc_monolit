import typing

from . import entities

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
