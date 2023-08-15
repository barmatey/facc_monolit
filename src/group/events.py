import typing
from src import core_types
from . import enums
from .entities import Group


class GroupCreated(core_types.Event):
    title: str
    category: enums.GroupCategory
    source_id: core_types.Id_
    ccols: list[str]
    fixed_ccols: list[str]
    sheet_id: typing.Optional[core_types.Id_] = None


class GroupGotten(core_types.Event):
    group_id: core_types.Id_


class GroupListGotten(core_types.Event):
    pass


class ParentUpdated(core_types.Event):
    group_instance: Group


class GroupSheetUpdated(core_types.Event):
    group_filter_by: dict


class GroupPartialUpdated(core_types.Event):
    title: typing.Optional[str] = None
    category: typing.Optional[enums.GroupCategory] = None
    columns: typing.Optional[list[str]] = None
    fixed_columns: typing.Optional[list[str]] = None
    id: typing.Optional[core_types.Id_] = None


class GroupDeleted(core_types.Event):
    group_id: core_types.Id_
