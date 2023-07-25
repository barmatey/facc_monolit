from sqlalchemy.orm import DeclarativeBase

from ..repository import Model, Entity


class PostgresModel(DeclarativeBase, Model):

    @classmethod
    def get_columns(cls) -> list[str]:
        # noinspection PyTypeChecker
        return [str(col.key) for col in cls.__table__.columns]

    def to_entity(self, **kwargs) -> Entity:
        raise NotImplemented
