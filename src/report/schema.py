import typing

import pydantic

from .. import core_types
from . import enums
from . import entities


class GroupCreateSchema(pydantic.BaseModel):
    title: str
    source_id: core_types.Id_
    columns: list[str]


class GroupRetrieveSchema(pydantic.BaseModel):
    id: core_types.Id_
    title: str
    category: enums.CategoryLiteral
    source_id: core_types.Id_
    sheet_id: core_types.Id_

    @classmethod
    def from_group_retrieve_entity(cls, data: entities.GroupRetrieve) -> typing.Self:
        params = {
            'id': data.id,
            'title': data.title,
            'category': data.category.name,
            'source_id': data.source_id,
            'sheet_id': data.sheet_id,
        }
        return cls(**params)
