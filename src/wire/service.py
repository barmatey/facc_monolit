import pandas as pd
from pandera.typing import DataFrame
from loguru import logger

from . import repository
from . import schema
from .. import core_types


class SourceService:
    repo: repository.SourceRepo = repository.SourceRepoPostgres

    async def create_source(self, data: schema.CreateSourceForm) -> core_types.Id_:
        source_id: core_types.Id_ = await self.repo().create_source(data)
        return source_id

    async def delete_source(self, id_: core_types.Id_) -> None:
        await self.repo().delete_source(id_)


class WireService:
    repo: repository.WireRepo = repository.WireRepoPostgres

    async def bulk_append_wire_from_csv(self, source_id: core_types.Id_,
                                        df: DataFrame[schema.WireSchema]) -> None:
        df = df.copy()
        df['source_base_id'] = source_id
        await self.repo().bulk_create_wire(df)
