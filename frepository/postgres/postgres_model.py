from ..repository import Model, Entity


class PostgresModel(Model):

    @classmethod
    def get_columns(cls) -> list[str]:
        raise NotImplemented

    def to_entity(self, **kwargs) -> Entity:
        raise NotImplemented
