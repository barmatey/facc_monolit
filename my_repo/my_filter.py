import typing

from sqlalchemy.orm import DeclarativeBase


class MyFilter:
    def __init__(self, data: dict, model: typing.Type[DeclarativeBase]):
        self.data = {key: value for key, value in data.items() if value is not None}
        self.model = model

    def get_filters(self) -> list:
        result = []
        for key, value in self.data.items():
            x = self.model.__table__.c[key] == value
            result.append(x)
        return result
