from websockets import typing

from src import core_types
from . import enums
from .entities import Group


class GroupCreated(core_types.Event):
    title: str
    category: enums.GroupCategory
    source_id: core_types.Id_
    ccols: list[str]
    fixed_ccols: list[str]
    sheet_id: core_types.Id_ = None


class GroupGotten(core_types.Event):
    group_id: core_types.Id_


class GroupListGotten(core_types.Event):
    pass


class ParentUpdated(core_types.Event):
    group_instance: Group


class GroupSheetUpdated(core_types.Event):
    group_filter_by: dict


class GroupPartialUpdated(core_types.Event):
    title: typing.Optional[str]
    category: typing.Optional[enums.GroupCategory]
    columns: typing.Optional[list[str]]
    fixed_columns: typing.Optional[list[str]]
    id: typing.Optional[core_types.Id_]


class GroupDeleted(core_types.Event):
    group_id: core_types.Id_
