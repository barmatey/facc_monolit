import typing

import pydantic

from .. import core_types
from . import schema


class Group(schema.GroupCreateForm):
    id_: core_types.Id_


class Report(schema.ReportCreateForm):
    id_: core_types.Id_
