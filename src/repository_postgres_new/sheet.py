import typing

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, delete, update
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from sheet import schema
from src import core_types
from src.sheet import entities
import db
from .base import BasePostgres, BaseRepo, ReturningEntity
from src.repository_postgres.normalizer import Normalizer, Denormalizer
from src.repository_postgres.sheet import Row, Col, Cell
from src.repository_postgres.sheet import Sheet as SheetModel


class SindexScrollSize(typing.TypedDict):
    count_sindexes: int
    scroll_size: int


class SindexRepo(BaseRepo):
    model: Row | Col = None

    async def retrieve_scroll_size_with_session(self, session: AsyncSession,
                                                sheet_id: core_types.Id_) -> SindexScrollSize:
        stmt = select(func.count()).where(self.model.sheet_id == sheet_id)
        count = await session.scalar(stmt)

        stmt = select(func.sum(self.model.size)).where(self.model.sheet_id == sheet_id, self.model.is_filtred)
        scroll_size = await session.scalar(stmt)

        sindex_scroll_size = SindexScrollSize(
            count_sindexes=count,
            scroll_size=scroll_size,
        )
        return sindex_scroll_size

    async def update_sindex_size(self, sheet_id: core_types.Id_, data: entities.UpdateSindexSize) -> None:
        async with db.get_async_session() as session:
            await self.update_with_session(session,
                                           filter_by={'sheet_id': sheet_id, 'id': data.sindex_id},
                                           data={'size': data.new_size})
            await self._update_scroll_pos_and_indexes_with_session(session, sheet_id)
            await session.commit()

    async def delete_bulk(self, sheet_id: core_types.Id_, sindex_ids: list[core_types.Id_]) -> None:
        async with db.get_async_session() as session:
            stmt = (
                delete(self.model)
                .where(self.model.sheet_id == sheet_id,
                       self.model.id.in_(sindex_ids),
                       ~self.model.is_freeze,
                       ~self.model.is_readonly,
                       )
            )
            _ = await session.execute(stmt)
            await self._update_scroll_pos_and_indexes_with_session(session, sheet_id)
            await session.commit()

    async def _update_scroll_pos_and_indexes_with_session(self, session: AsyncSession,
                                                          sheet_id: core_types.Id_) -> None:
        # Get data
        filter_by = {"sheet_id": sheet_id}
        order_by = 'index'
        sindexes = await self.retrieve_bulk_as_dataframe_with_session(session, filter_by, order_by)
        sindexes = sindexes.rename({"id": "sindex_id"}, axis=1)

        # Calculate new values
        sindexes.loc[~sindexes['is_filtred'] | sindexes['is_freeze'], 'size'] = 0
        sindexes['scroll_pos'] = sindexes['size'].cumsum().shift(1).fillna(0)
        sindexes.loc[sindexes['is_freeze'], 'scroll_pos'] = -1
        sindexes['index_value'] = range(0, len(sindexes.index))

        # Update data
        values: list[dict] = sindexes[['sindex_id', 'is_filtred', 'scroll_pos', 'index_value']].to_dict(
            orient='records')
        stmt = (
            self.model.__table__.update()
            .where(self.model.__table__.c.id == bindparam('sindex_id'))
            .values(
                {
                    "scroll_pos": bindparam("scroll_pos"),
                    "is_filtred": bindparam("is_filtred"),
                    "index": bindparam("index_value"),
                })
        )
        _ = await session.execute(stmt, values)


class RowRepo(SindexRepo):
    model = Row


class ColRepo(SindexRepo):
    model = Col


class CellRepo(BaseRepo):
    model = Cell

    async def update(self, sheet_id: core_types.Id_, data: entities.UpdateCell) -> None:
        async with db.get_async_session() as session:
            data = data.dict()
            data = {key: data[key] for key in data if data[key] is not None}
            filter_ = {'id': data.pop('id')} | {'sheet_id': sheet_id, 'is_readonly': False}
            _ = await self.update_with_session(session, filter_by=filter_, data=data)
            await session.commit()

    async def update_many(self, sheet_id: core_types.Id_, data: list[entities.Cell]) -> None:
        values = []
        for cell in data:
            c = cell.copy()
            c['cell_id'] = c.pop('id')
            c['cell_value'] = c.pop('value')
            c['cell_dtype'] = c.pop('dtype')
            values.append(c)

        async with db.get_async_session() as session:
            # Update
            stmt = (
                self.model.__table__.update()
                .where(self.model.sheet_id == sheet_id,
                       ~self.model.is_readonly,
                       self.model.id == bindparam('cell_id'),
                       )
                .values({
                    "value": bindparam("cell_value"),
                    "dtype": bindparam("cell_dtype"),
                })
            )
            _ = await session.execute(stmt, values)
            await session.commit()


class SheetRepo(BasePostgres):
    model = SheetModel
    normalizer = Normalizer
    denormalizer = Denormalizer

    def __init__(self,
                 session: AsyncSession,
                 returning: ReturningEntity = "ENTITY",
                 fields: list[str] = None,
                 scalars: bool = False,
                 ):
        super().__init__(session, returning, fields, scalars)
        self.__cell_repo = CellRepo()
        self.__row_repo = RowRepo()
        self.__col_repo = ColRepo()

    async def overwrite_with_session(self, session: AsyncSession, sheet_id: core_types.Id_,
                                     data: entities.SheetCreate) -> None:
        # Delete old data
        filter_by = {"sheet_id": sheet_id}
        await self.row_repo().delete_bulk_with_session(session, filter_by)
        await self.col_repo().delete_bulk_with_session(session, filter_by)
        await self.cell_repo().delete_bulk_with_session(session, filter_by)
        # Create new data
        await self._create_rows_cols_and_cells(session, sheet_id, data)

    async def retrieve_scroll_size(self, id_: core_types.Id_) -> entities.ScrollSize:
        async with db.get_async_session() as session:
            row: SindexScrollSize = await self.row_repo().retrieve_scroll_size_with_session(session, sheet_id=id_)
            col: SindexScrollSize = await self.col_repo().retrieve_scroll_size_with_session(session, sheet_id=id_)

            scroll_size = entities.ScrollSize(
                count_rows=row['count_sindexes'],
                count_cols=col['count_sindexes'],
                scroll_height=row['scroll_size'],
                scroll_width=col['scroll_size'],
            )
            return scroll_size

    async def _create_rows_cols_and_cells(self, sheet_id: core_types.Id_, data: entities.SheetCreate) -> None:
        session = self.__session

        # Create row, col and cell data from denormalized dataframe
        normalizer = self.normalizer(**data.dict())
        normalizer.normalize()

        # Create sindexes, save created ids for following create cells
        rows = normalizer.get_normalized_rows().assign(sheet_id=sheet_id).to_dict(orient='records')
        cols = normalizer.get_normalized_cols().assign(sheet_id=sheet_id).to_dict(orient='records')

        row_ids = await self.__row_repo.create_bulk_with_session(session, rows)
        col_ids = await self.__col_repo.create_bulk_with_session(session, cols)

        # Create cells
        repeated_row_ids = np.repeat(row_ids, len(col_ids))
        repeated_col_ids = col_ids * len(row_ids)
        cells = normalizer.get_normalized_cells().assign(
            sheet_id=sheet_id, row_id=repeated_row_ids, col_id=repeated_col_ids).to_dict(orient='records')
        _ = await self.__cell_repo.create_bulk_with_session(session, cells)

    @staticmethod
    def _merge_into_sheet_entity(sheet_id, rows, cols, cells, ) -> entities.Sheet:
        saved_cols = cells.columns.copy()
        cells = pd.merge(cells, rows[['id', 'index', ]], left_on='row_id', right_on='id', suffixes=('', '_row'))
        cells = pd.merge(cells, cols[['id', 'index', ]], left_on='col_id', right_on='id', suffixes=('', '_col'))
        cells = cells.sort_values(['index', 'index_col'])[saved_cols]

        sheet = entities.Sheet(
            id=sheet_id,
            rows=rows.to_dict(orient='records'),
            cols=cols.to_dict(orient='records'),
            cells=cells.to_dict(orient='records'),
        )
        return sheet

    async def create_one(self, data: schema.SheetCreateSchema):
        sheet: SheetModel = await super().create_one({})
        await self._create_rows_cols_and_cells(sheet.id, data)
        return super()._parse_result_one(sheet)

    async def get_one(self, data: entities.SheetRetrieve) -> entities.Sheet:
        session = self.__session

        filter_by = {"sheet_id": data.sheet_id, "is_filtred": True, }
        order_by = 'index'
        rows = await self.inner_repo(Row, returning="FRAME", session=session).get_many(filter_by, order_by)
        cols = await self.inner_repo(Col, returning="FRAME", session=session).get_many(filter_by, order_by)
        cells = await self.inner_repo(Cell, returning="FRAME", session=session).get_many(filter_by)
        return self._merge_into_sheet_entity(data.sheet_id, rows, cols, cells)

    async def get_one_as_frame(self, data: entities.SheetRetrieve) -> pd.DataFrame:
        async with super().get_async_session() as session:
            filter_by = {"sheet_id": data.sheet_id}
            cells = await self.inner_repo(Cell, returning="FRAME", session=session).get_many(filter_by)
            rows = await self.inner_repo(Row, returning="FRAME", session=session).get_many(filter_by)
            cols = await self.inner_repo(Col, returning="FRAME", session=session).get_many(filter_by)

            denormalizer = self.denormalizer(rows, cols, cells)
            denormalizer.denormalize()
            df = denormalizer.get_denormalized()
            return df


class SheetFilterRepo:
    row_model = Row
    cell_model = Cell

    async def retrieve_col_filter(self, sheet_id: core_types.Id_, col_id: core_types.Id_) -> entities.ColFilter:
        async with db.get_async_session() as session:
            items = await session.execute(
                select(self.cell_model.value, self.cell_model.dtype, self.cell_model.is_filtred)
                .distinct()
                .filter_by(sheet_id=sheet_id, col_id=col_id, is_index=False)
                .order_by(self.cell_model.value)
            )
            columns = [self.cell_model.value.key, self.cell_model.dtype.key, self.cell_model.is_filtred.key]
            items = pd.DataFrame.from_records(items.fetchall(), columns=columns).to_dict(orient='records')
            col_filter = entities.ColFilter(col_id=col_id, sheet_id=sheet_id,
                                            items=[entities.FilterItem(**x) for x in items])
            return col_filter

    async def update_col_filter(self, data: entities.ColFilter) -> None:
        async with db.get_async_session() as session:
            await self._update_filtred_flag_in_cells_with_session(session, data)
            await self._update_filtred_flag_and_scroll_pos_in_rows_with_session(session, sheet_id=data.sheet_id)
            await session.commit()

    async def clear_all_filters(self, sheet_id: core_types.Id_) -> None:
        async with db.get_async_session() as session:
            stmt = update(self.cell_model).where(self.cell_model.sheet_id == sheet_id)
            await session.execute(stmt, {"is_filtred": True})
            await self._update_filtred_flag_and_scroll_pos_in_rows_with_session(session, sheet_id=sheet_id)
            await session.commit()

    async def _retrieve_total_rows_with_session(self, session: AsyncSession,
                                                sheet_id: core_types.Id_) -> pd.DataFrame:
        stmt = (
            select(
                self.row_model.__table__.c.id,
                self.row_model.__table__.c.size,
                self.row_model.__table__.c.is_freeze,
                self.row_model.__table__.c.scroll_pos, )
            .filter_by(sheet_id=sheet_id)
            .order_by(self.row_model.__table__.c.index)
        )
        rows = await session.execute(stmt)
        rows = pd.DataFrame.from_records(rows.fetchall(), columns=['row_id', 'size', 'is_freeze', 'scroll_pos'])
        return rows

    async def _retrieve_filtred_rows_with_session(self, session: AsyncSession,
                                                  sheet_id: core_types.Id_) -> pd.DataFrame:
        stmt = (
            select(self.cell_model.row_id, func.count(self.cell_model.is_filtred)
                   .filter(~self.cell_model.is_filtred))
            .group_by(self.cell_model.row_id)
            .filter(self.cell_model.sheet_id == sheet_id)
        )
        result = await session.execute(stmt)
        filtred_rows = pd.DataFrame.from_records(result.fetchall(), columns=['row_id', 'is_filtred'])
        filtred_rows['is_filtred'] = ~filtred_rows['is_filtred'].astype(bool)
        return filtred_rows

    async def _update_filtred_flag_and_scroll_pos_in_rows_with_session(self, session: AsyncSession,
                                                                       sheet_id: core_types.Id_) -> None:
        rows = await self._retrieve_total_rows_with_session(session, sheet_id)
        filtred_rows = await self._retrieve_filtred_rows_with_session(session, sheet_id)

        if len(rows) != len(filtred_rows):
            raise Exception

        rows = pd.merge(rows, filtred_rows, on='row_id', how='inner')

        rows.loc[~rows['is_filtred'] | rows['is_freeze'], 'size'] = 0
        rows['scroll_pos'] = rows['size'].cumsum().shift(1).fillna(0)
        rows.loc[rows['is_freeze'], 'scroll_pos'] = -1

        values: list[dict] = rows[['row_id', 'is_filtred', 'scroll_pos']].to_dict(orient='records')
        stmt = (
            self.row_model.__table__.update()
            .where(self.row_model.__table__.c.id == bindparam('row_id'))
            .values({"scroll_pos": bindparam("scroll_pos"), "is_filtred": bindparam("is_filtred")})
        )
        _ = await session.execute(stmt, values)

    async def _update_filtred_flag_in_cells_with_session(self, session: AsyncSession, data: entities.ColFilter) -> None:
        # Convert input data because "value" is reserved word in bindparam function
        items = [{
            'cell_value': x.value,
            'is_filtred': x.is_filtred,
            'dtype': x.dtype,
        } for x in data.items]

        stmt = (
            self.cell_model.__table__.update()
            .where(self.cell_model.__table__.c.value == bindparam('cell_value'),
                   self.cell_model.col_id == data.col_id)
            .values({"is_filtred": bindparam("is_filtred")})
        )
        _ = await session.execute(stmt, items)


class SheetSorterRepo:
    row_model = Row
    cell_model = Cell

    async def update_col_sorter(self, data: entities.ColSorter) -> None:
        async with db.get_async_session() as session:
            sorted_rows = await self._retrieve_sorted_rows_with_session(session, data.sheet_id, data.col_id,
                                                                        data.ascending)
            filtred_rows = await self._retrieve_filtred_rows_with_session(session, data.sheet_id)
            await self._update_row_index_and_scroll_pos_with_session(session, sorted_rows, filtred_rows)
            await session.commit()

    async def _update_row_index_and_scroll_pos_with_session(self, session: AsyncSession,
                                                            sorted_rows: pd.DataFrame,
                                                            filtred_rows: pd.DataFrame) -> None:
        merged = pd.merge(filtred_rows, sorted_rows, on='row_id', how='left').sort_values('row_index')
        merged.loc[merged['is_index'], 'size'] = 0
        merged['scroll_pos'] = merged['size'].cumsum().shift(1).fillna(0)
        merged.loc[merged['is_index'], 'scroll_pos'] = -1

        values = merged[['row_id', 'row_index', 'scroll_pos']].to_dict(orient='records')

        stmt = (
            self.row_model.__table__.update()
            .where(self.row_model.__table__.c.id == bindparam('row_id'))
            .values({"scroll_pos": bindparam("scroll_pos"), "index": bindparam("row_index")})
        )
        _ = await session.execute(stmt, values)

    async def _retrieve_filtred_rows_with_session(self, session: AsyncSession,
                                                  sheet_id: core_types.Id_) -> pd.DataFrame:
        stmt = (
            select(
                self.row_model.__table__.c.id,
                self.row_model.__table__.c.size
            )
            .where(
                self.row_model.sheet_id == sheet_id,
                self.row_model.__table__.c.is_filtred)
        )
        rows = await session.execute(stmt)
        rows = pd.DataFrame.from_records(rows.fetchall(), columns=['row_id', 'size'])
        return rows

    async def _retrieve_sorted_rows_with_session(self, session: AsyncSession, sheet_id: core_types.Id_,
                                                 col_id: core_types.Id_, asc: bool) -> pd.DataFrame:
        # Retrieving
        stmt = (
            select(
                self.cell_model.__table__.c.row_id,
                self.cell_model.__table__.c.value,
                self.cell_model.__table__.c.is_index)
            .where(
                self.cell_model.sheet_id == sheet_id,
                self.cell_model.col_id == col_id, )
        )
        result = await session.execute(stmt)

        # Sorting
        result = pd.DataFrame.from_records(result.fetchall(), columns=['row_id', 'cell_value', 'is_index'])
        freeze_part = result.loc[result['is_index']]
        free_part = result.loc[~result['is_index']].sort_values('cell_value', ascending=asc)
        sorted_df = pd.concat([freeze_part, free_part], ignore_index=True)
        sorted_df['row_index'] = range(len(sorted_df))
        return sorted_df
