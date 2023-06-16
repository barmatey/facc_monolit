import pydantic
from .. import entities


class Source(entities.Source):
    pass


class CreateSourceForm(pydantic.BaseModel):
    title: str
