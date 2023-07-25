import typing

from sqlalchemy.orm import DeclarativeBase

ReturnValue = typing.Literal['MODEL', 'ENTITY', 'FIELD', 'NONE', ]


class MyReturn:
    def __init__(self, model: typing.Type[DeclarativeBase], return_value: ReturnValue, model_keys: list[str] = None):
        self.model = model
        self.return_value = return_value
        self.model_keys = model_keys

    def get_return(self):
        pass
