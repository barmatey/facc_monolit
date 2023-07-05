import typing

import pydantic

Id_ = int
MongoId = str
OrderBy = typing.Union[str, list[str]]
DTO = typing.Union[pydantic.BaseModel, dict]
