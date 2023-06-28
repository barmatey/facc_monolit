import pydantic
from . import entities


class SourceSchema(entities.Source):
    pass


class SourceCreateSchema(entities.SourceCreate):
    pass
