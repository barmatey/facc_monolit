import pydantic

from .. import core_types
from . import enums


class GroupCreateForm(pydantic.BaseModel):
    title: str
    category: enums.CategoryLiteral
    source_id: core_types.Id_


class GroupResponse(pydantic.BaseModel):
    id: core_types.Id_
    title: str
    category: enums.CategoryLiteral
    source_id: core_types.Id_
