import pydantic


class CreateSourceForm(pydantic.BaseModel):
    title: str
