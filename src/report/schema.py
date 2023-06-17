import pydantic

from .. import core_types
from . import enums


class GroupCreateSchema(pydantic.BaseModel):
    title: str
    source_id: core_types.Id_
    columns: list[str]


class GroupRetrieveSchema(pydantic.BaseModel):
    id: core_types.Id_
    title: str
    category: enums.CategoryLiteral
    source_id: core_types.Id_
