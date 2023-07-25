import typing

from sqlalchemy import bindparam
from sqlalchemy.orm import DeclarativeBase


class MyFilter:
    def __init__(self, data: dict, model: typing.Type[DeclarativeBase]):
        self.data = {key: value for key, value in data.items() if value is not None}
        self.model = model

    def get_filters(self) -> list:
        result = []
        for key, value in self.data.items():
            if value.startswith('$linked:'):
                value = value.split('$linked')[1]
                x = self.model.__table__.c[key] == bindparam(value)
            else:
                x = self.model.__table__.c[key] == value
            result.append(x)
        return result
