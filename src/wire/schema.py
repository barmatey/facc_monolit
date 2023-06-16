import pydantic
from .. import models


class Source(models.Source):
    pass


class CreateSourceForm(pydantic.BaseModel):
    title: str
