import typing

from . import entities


class SheetRetrieveSchema(entities.SheetRetrieve):
    pass


class SheetSchema(entities.Sheet):
    pass


class ScrollSizeSchema(entities.ScrollSize):
    @classmethod
    def from_scroll_size_entity(cls, data: entities.ScrollSize):
        return cls(
            count_cols=data.count_cols,
            count_rows=data.count_rows,
            scroll_width=data.scroll_width,
            scroll_height=data.scroll_height,
        )


class ColFilterRetrieveSchema(entities.ColFilterRetrieve):
    pass


class ColFilterSchema(entities.ColFilter):
    pass


class ColSorterSchema(entities.ColSorter):
    from_scroll: int
    to_scroll: int


class CopySindexSchema(entities.CopySindex):
    pass


class CellSchema(entities.Cell):
    pass


class CopyCellSchema(entities.CopyCell):
    pass


class UpdateSindexSizeSchema(entities.UpdateSindexSize):
    pass


class UpdateCellSchema(entities.UpdateCell):
    pass
