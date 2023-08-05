import typing

from pydantic import BaseModel

from src import core_types
from . import entities, enums


class UpdateSindexSizeSchema(BaseModel):
    new_size: int
    sindex_id: core_types.Id_


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
