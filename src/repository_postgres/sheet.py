import typing

import numpy as np
import pandas as pd
from sqlalchemy import ForeignKey, Integer, Boolean, String, bindparam, delete, update
from sqlalchemy import func, select, or_, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from src import core_types
from src.sheet import entities
from . import db
from .base import BaseRepo, BaseModel
from repository_postgres_new.normalizer import Normalizer, Denormalizer


class Sheet(BaseModel):
    __tablename__ = "sheet"

    def to_entity(self, **kwargs) -> entities.Sheet:
        raise NotImplemented


class Row(BaseModel):
    __tablename__ = "sheet_row"
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_readonly: Mapped[int] = mapped_column(Boolean, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False, index=True)


class Col(BaseModel):
    __tablename__ = "sheet_col"
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_readonly: Mapped[int] = mapped_column(Boolean, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False, index=True)


class Cell(BaseModel):
    __tablename__ = "sheet_cell"
    value: Mapped[str] = mapped_column(String(1000), nullable=True, index=True)
    dtype: Mapped[str] = mapped_column(String(30), nullable=False)
    is_readonly: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_index: Mapped[bool] = mapped_column(Boolean, nullable=False)
    color: Mapped[str] = mapped_column(String(16), nullable=True)
    text_align: Mapped[str] = mapped_column(String(8), default='left')
    row_id: Mapped[int] = mapped_column(Integer, ForeignKey(Row.id, ondelete='CASCADE'), nullable=False, index=True)
    col_id: Mapped[int] = mapped_column(Integer, ForeignKey(Col.id, ondelete='CASCADE'), nullable=False, index=True)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False, index=True)


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


class SheetRepo(BaseRepo):
    model = Sheet
    row_repo = RowRepo
    col_repo = ColRepo
    cell_repo = CellRepo
    normalizer = Normalizer
    denormalizer = Denormalizer

    async def create_with_session(self, session: AsyncSession, data: entities.SheetCreate) -> core_types.Id_:
        sheet: Sheet = await super().create_with_session(session, {})
        sheet_id = sheet.id
        await self._create_rows_cols_and_cells(session, sheet_id, data)
        return sheet_id

    async def overwrite_with_session(self, session: AsyncSession, sheet_id: core_types.Id_,
                                     data: entities.SheetCreate) -> None:
        # Delete old data
        filter_by = {"sheet_id": sheet_id}
        await self.row_repo().delete_bulk_with_session(session, filter_by)
        await self.col_repo().delete_bulk_with_session(session, filter_by)
        await self.cell_repo().delete_bulk_with_session(session, filter_by)
        # Create new data
        await self._create_rows_cols_and_cells(session, sheet_id, data)

    async def retrieve_as_sheet(self, data: entities.SheetRetrieve) -> entities.Sheet:
        if data.from_scroll is None or data.to_scroll is None:
            return await self._retrieve_as_sheet_without_pagination(data)
        return await self._retrieve_as_sheet_with_pagination(data)

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

    async def _create_rows_cols_and_cells(self, session: AsyncSession,
                                          sheet_id: core_types.Id_, data: entities.SheetCreate) -> None:
        # Create row, col and cell data from denormalized dataframe
        normalizer = self.normalizer(**data.dict())
        normalizer.normalize()

        # Create sindexes, save created ids for following create cells
        rows = normalizer.get_normalized_rows().assign(sheet_id=sheet_id).to_dict(orient='records')
        cols = normalizer.get_normalized_cols().assign(sheet_id=sheet_id).to_dict(orient='records')

        row_ids = await self.row_repo().create_bulk_with_session(session, rows)
        col_ids = await self.col_repo().create_bulk_with_session(session, cols)

        # Create cells
        repeated_row_ids = np.repeat(row_ids, len(col_ids))
        repeated_col_ids = col_ids * len(row_ids)
        cells = normalizer.get_normalized_cells().assign(
            sheet_id=sheet_id, row_id=repeated_row_ids, col_id=repeated_col_ids).to_dict(orient='records')
        _ = await self.cell_repo().create_bulk_with_session(session, cells)

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

    async def _retrieve_as_sheet_without_pagination(self, data: entities.SheetRetrieve) -> entities.Sheet:
        async with db.get_async_session() as session:
            filter_by = {"sheet_id": data.sheet_id, "is_filtred": True, }
            order_by = 'index'
            rows = await self.row_repo().retrieve_bulk_as_dataframe_with_session(session, filter_by, order_by)
            cols = await self.col_repo().retrieve_bulk_as_dataframe_with_session(session, filter_by, order_by)
            cells = await self.cell_repo().retrieve_bulk_as_dataframe_with_session(session, filter_by)
            return self._merge_into_sheet_entity(data.sheet_id, rows, cols, cells)

    async def _retrieve_as_sheet_with_pagination(self, data: entities.SheetRetrieve) -> entities.Sheet:
        async with db.get_async_session() as session:
            # Find rows
            stmt = (
                select(self.row_repo.model.__table__)
                .where(
                    self.row_repo.model.sheet_id == data.sheet_id,
                    self.row_repo.model.is_filtred,
                    or_(
                        and_(self.row_repo.model.scroll_pos >= data.from_scroll,
                             self.row_repo.model.scroll_pos < data.to_scroll),
                        self.row_repo.model.is_freeze,
                    )
                )
                .order_by(self.row_repo.model.scroll_pos)
            )
            result = await session.execute(stmt)
            rows = pd.DataFrame.from_records(result.fetchall(), columns=Row.get_columns())

            # Find Cols
            stmt = (
                select(self.col_repo.model.__table__)
                .where(self.col_repo.model.sheet_id == data.sheet_id)
                .order_by(self.col_repo.model.scroll_pos)
            )
            result = await session.execute(stmt)
            cols = pd.DataFrame.from_records(result.fetchall(), columns=Col.get_columns())

            if len(rows['id']) > 32_000 or len(cols['id']) > 32_000:
                raise ValueError('to many conditions for request')

            # Find cells
            stmt = (
                select(self.cell_repo.model.__table__)
                .where(self.cell_repo.model.sheet_id == data.sheet_id,
                       self.cell_repo.model.row_id.in_(rows['id'].tolist()),
                       self.cell_repo.model.col_id.in_(cols['id'].tolist()),
                       )
            )
            result = await session.execute(stmt)
            cells = pd.DataFrame.from_records(result.fetchall(), columns=Cell.get_columns())

            return self._merge_into_sheet_entity(data.sheet_id, rows, cols, cells)

    async def retrieve_as_dataframe_with_session(self, session: AsyncSession, sheet_id: core_types.Id_) -> pd.DataFrame:
        cells = await self.cell_repo().retrieve_bulk_as_dataframe_with_session(session, {"sheet_id": sheet_id})
        rows = await self.row_repo().retrieve_bulk_as_dataframe_with_session(session, {"sheet_id": sheet_id})
        cols = await self.col_repo().retrieve_bulk_as_dataframe_with_session(session, {"sheet_id": sheet_id})

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
