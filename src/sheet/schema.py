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


class ColFilterSchema(entities.ColFilter):
    @classmethod
    def from_col_filter_entity(cls, data: entities.ColFilter) -> typing.Self:
        return cls(
            col_id=data.col_id,
            items=data.items,
        )
