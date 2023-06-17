import typing

from pydantic import BaseModel
from .. import core_types


class SheetRetrieveSchema(BaseModel):
    sheet_id: core_types.Id_
    from_scroll_pos: typing.Optional[int]
    to_scroll_pos: typing.Optional[int]
