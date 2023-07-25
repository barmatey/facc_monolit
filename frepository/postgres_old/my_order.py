import typing

from sqlalchemy.orm import DeclarativeBase

OrderBy = typing.Union[str, list[str]]


class MyOrder:
    def __init__(self, order_by: OrderBy | None, asc: bool, model: typing.Type[DeclarativeBase]):
        self.order_by = order_by
        self.asc = asc
        self.model = model

    def get_sorter(self) -> list:
        if self.order_by is None:
            return []
        if isinstance(self.order_by, str):
            if self.asc:
                return [self.model.__table__.c[self.order_by].asc()]
            else:
                return [self.model.__table__.c[self.order_by].desc()]
        if self.asc:
            return [self.model.__table__.c[col].asc() for col in self.order_by]
        else:
            return [self.model.__table__.c[col].desc() for col in self.order_by]
