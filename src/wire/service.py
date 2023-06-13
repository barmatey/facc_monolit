from . import repository
from . import schema
from .. import core_types


class SourceService:
    repo: repository.SourceRepo = repository.SourceRepoPostgres

    async def create_source(self, data: schema.CreateSourceForm) -> core_types.Id_:
        source_id: core_types.Id_ = await self.repo().create_source(data)
        return source_id
