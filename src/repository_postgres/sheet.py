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
from .normalizer import Normalizer, Denormalizer


class Sheet(BaseModel):
    __tablename__ = "sheet"

    def to_entity(self, **kwargs) -> entities.Sheet:
        raise NotImplemented


class Row(BaseModel):
    __tablename__ = "sheet_row"
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(Sheet.id, ondelete='CASCADE'), nullable=False, index=True)


class Col(BaseModel):
    __tablename__ = "sheet_col"
    size: Mapped[int] = mapped_column(Integer, nullable=False)
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

        stmt = select(func.max(self.model.scroll_pos)).where(self.model.sheet_id == sheet_id)
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
            await self._update_scroll_pos_with_session(session, sheet_id)
            await session.commit()

    async def delete_bulk(self, sheet_id: core_types.Id_, sindex_ids: list[core_types.Id_]) -> None:
        async with db.get_async_session() as session:
            stmt = (
                delete(self.model)
                .where(self.model.sheet_id == sheet_id,
                       self.model.id.in_(sindex_ids))
            )
            _ = await session.execute(stmt)
            await self._update_scroll_pos_with_session(session, sheet_id)
            await session.commit()

    async def _update_scroll_pos_with_session(self, session: AsyncSession, sheet_id: core_types.Id_) -> None:
        # Get data
        sindexes = await self.retrieve_bulk_as_dataframe_with_session(session,
                                                                      filter_by={"sheet_id": sheet_id},
                                                                      order_by="index")
        sindexes = sindexes[['id', 'size', 'is_freeze', 'is_filtred', 'scroll_pos']].rename({"id": "sindex_id"}, axis=1)

        # Calculate new values
        sindexes.loc[~sindexes['is_filtred'] | sindexes['is_freeze'], 'size'] = 0
        sindexes['scroll_pos'] = sindexes['size'].cumsum().shift(1).fillna(0)
        sindexes.loc[sindexes['is_freeze'], 'scroll_pos'] = -1

        # Update data
        values: list[dict] = sindexes[['sindex_id', 'is_filtred', 'scroll_pos']].to_dict(orient='records')
        stmt = (
            self.model.__table__.update()
            .where(self.model.__table__.c.id == bindparam('sindex_id'))
            .values({"scroll_pos": bindparam("scroll_pos"), "is_filtred": bindparam("is_filtred")})
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

        return sheet_id

    async def retrieve_as_sheet(self, data: entities.SheetRetrieve) -> entities.Sheet:
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

            # Sort cells
            saved_cols = cells.columns.copy()
            cells = pd.merge(cells, rows[['id', 'index', ]], left_on='row_id', right_on='id', suffixes=('', '_row'))
            cells = pd.merge(cells, cols[['id', 'index', ]], left_on='col_id', right_on='id', suffixes=('', '_col'))
            cells = cells.sort_values(['index', 'index_col'])[saved_cols]

            sheet = entities.Sheet(
                id=data.sheet_id,
                rows=rows.to_dict(orient='records'),
                cols=cols.to_dict(orient='records'),
                cells=cells.to_dict(orient='records'),
            )
            return sheet

    async def retrieve_as_dataframe_with_session(self, session: AsyncSession, sheet_id: core_types.Id_) -> pd.DataFrame:
        cells = await self.cell_repo().retrieve_bulk_as_dataframe_with_session(session, {"sheet_id": sheet_id})
        rows = await self.row_repo().retrieve_bulk_as_dataframe_with_session(session, {"sheet_id": sheet_id})
        cols = await self.col_repo().retrieve_bulk_as_dataframe_with_session(session, {"sheet_id": sheet_id})

        denormalizer = self.denormalizer(rows, cols, cells)
        denormalizer.denormalize()
        df = denormalizer.get_denormalized()
        return df

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
            col_filter = entities.ColFilter(col_id=col_id, sheet_id=sheet_id, items=items)
            return col_filter

    async def update_col_filter(self, data: entities.ColFilter) -> None:
        async with db.get_async_session() as session:
            await self._update_filtred_flag_in_cells_with_session(session, data)
            await self._update_filtred_flag_and_scroll_pos_in_rows_with_session(session, sheet_id=data['sheet_id'])
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
        items = (
            pd.DataFrame(data['items'])
            .rename({'value': 'cell_value'}, axis=1)
            .to_dict(orient='records')
        )

        stmt = (
            self.cell_model.__table__.update()
            .where(self.cell_model.__table__.c.value == bindparam('cell_value'),
                   self.cell_model.col_id == data['col_id'])
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


class SheetTableRepo:
    cell_model = Cell
    row_model = Row
    col_model = Col

    async def copy_cells(self, sheet_id: core_types.Id_, copy_from: list[entities.CopyCell],
                         copy_to: list[entities.CopyCell]):
        async with db.get_async_session() as session:
            # Change row and col indexes
            if len(copy_from) == 1:
                records = [x.dict() for x in copy_to]
                table = pd.DataFrame(records)
                table['value'] = copy_from[0].value
                table['dtype'] = copy_from[0].dtype
            else:
                records = [x.dict() for x in copy_from]
                table = pd.DataFrame(records)
                row_offset = copy_from[0].row_index - copy_to[0].row_index
                col_offset = copy_from[0].col_index - copy_to[0].col_index
                table['row_index'] = table['row_index'] - row_offset
                table['col_index'] = table['col_index'] - col_offset

            # Find target row ids
            stmt = (
                select(self.row_model.__table__.c.id,
                       self.row_model.__table__.c.index)
                .where(self.row_model.sheet_id == sheet_id,
                       self.row_model.index.in_(table['row_index']))
            )
            result = await session.execute(stmt)
            target_rows = pd.DataFrame.from_records(result.fetchall(), columns=['row_id_target', 'row_index'])

            # Find target col ids
            stmt = (
                select(self.col_model.__table__.c.id,
                       self.col_model.__table__.c.index)
                .where(self.col_model.sheet_id == sheet_id,
                       self.col_model.index.in_(table['col_index']))
            )
            result = await session.execute(stmt)
            target_cols = pd.DataFrame.from_records(result.fetchall(), columns=['col_id_target', 'col_index'])

            # Update
            table = table.merge(target_rows, on='row_index').merge(target_cols, on='col_index')
            values = table[['value', 'dtype', 'row_id_target', 'col_id_target', 'sheet_id']].to_dict(orient='records')
            stmt = (
                self.cell_model.__table__.update()
                .where(self.cell_model.sheet_id == sheet_id,
                       ~self.cell_model.is_readonly,
                       self.cell_model.row_id == bindparam('row_id_target'),
                       self.cell_model.col_id == bindparam('col_id_target'), )
                .values({
                    "value": bindparam("value"),
                    "dtype": bindparam("dtype"),
                })
            )
            _ = await session.execute(stmt, values)
            await session.commit()

            await session.commit()

    async def copy_rows(self, sheet_id: core_types.Id_,
                        copy_from: list[entities.CopySindex], copy_to: list[entities.CopySindex]) -> None:
        async with db.get_async_session() as session:
            if len(copy_from) == 1:
                await self._copy_rows_one_to_many(session, sheet_id, copy_from, copy_to)
            else:
                await self._copy_rows_many_to_one(session, sheet_id, copy_from, copy_to)
            await session.commit()

    async def copy_cols(self, sheet_id: core_types.Id_,
                        copy_from: list[entities.CopySindex], copy_to: list[entities.CopySindex]) -> None:
        async with db.get_async_session() as session:
            if len(copy_from) == 1:
                await self._copy_cols_one_to_many(session, sheet_id, copy_from, copy_to)
            else:
                await self._copy_cols_many_to_one(session, sheet_id, copy_from, copy_to)
            await session.commit()

    async def _copy_rows_one_to_many(self, session: AsyncSession, sheet_id: core_types.Id_,
                                     copy_from: list[entities.CopySindex], copy_to: list[entities.CopySindex]) -> None:
        records = [{'row_id': x.id, 'row_index': x.index, 'sheet_id': x.sheet_id} for x in copy_to]
        to_row_df = pd.DataFrame(records)

        from_cell_df = await self._retrieve_cell_df(session, sheet_id, self.cell_model.row_id == copy_from[0].id)
        to_cell_df = await self._retrieve_cell_df(session, sheet_id, self.cell_model.row_id.in_(to_row_df['row_id']))
        table = pd.merge(
            from_cell_df[['value', 'dtype', 'col_id', 'sheet_id']],
            to_cell_df[['row_id', 'col_id', 'sheet_id']],
            on=['col_id', 'sheet_id']
        )
        await self._update_cells_with_bindparams(session, table, {'value': 'value', 'dtype': 'dtype'})

    async def _copy_rows_many_to_one(self, session: AsyncSession, sheet_id: core_types.Id_,
                                     copy_from: list[entities.CopySindex], copy_to: list[entities.CopySindex]) -> None:
        records = [{'row_id': x.id, 'row_index': x.index, 'sheet_id': x.sheet_id} for x in copy_from]
        from_row_df = pd.DataFrame(records)

        # Offset indexes in from_row_df then retrieve to_row_df searching via offseted indexes
        row_offset = copy_from[0].index - copy_to[0].index
        from_row_df['row_index'] = from_row_df['row_index'] - row_offset
        to_row_df = await self._retrieve_row_df(session, sheet_id,
                                                self.row_model.index.in_(from_row_df['row_index']))
        to_row_df = to_row_df.rename({'id': 'row_id', 'index': 'row_index'}, axis=1)

        # Find cells and merge with rows
        from_cell = await self._retrieve_cell_df(session, sheet_id, self.cell_model.row_id.in_(from_row_df['row_id']))
        from_cell = pd.merge(from_cell, from_row_df, on=['row_id', 'sheet_id'])

        to_cell = await self._retrieve_cell_df(session, sheet_id, self.cell_model.row_id.in_(to_row_df['row_id']))
        to_cell = pd.merge(to_cell, to_row_df, on='row_id')

        # Create table with data and update cells
        table = pd.merge(
            from_cell[['value', 'dtype', 'col_id', 'row_index', 'sheet_id']],
            to_cell[['row_id', 'col_id', 'row_index', 'sheet_id']],
            on=['row_index', 'col_id', 'sheet_id'])

        await self._update_cells_with_bindparams(session, table, {'value': 'value', 'dtype': 'dtype'})

    async def _copy_cols_many_to_one(self, session: AsyncSession, sheet_id: core_types.Id_,
                                     copy_from: list[entities.CopySindex], copy_to: list[entities.CopySindex]) -> None:
        records = [{'col_id': x.id, 'col_index': x.index, 'sheet_id': x.sheet_id} for x in copy_from]
        from_col_df = pd.DataFrame(records)

        # Offset indexes in from_col_df then retrieve to_col_df searching via offseted indexes
        row_offset = copy_from[0].index - copy_to[0].index
        from_col_df['col_index'] = from_col_df['col_index'] - row_offset
        to_col_df = await self._retrieve_col_df(session, sheet_id,
                                                self.col_model.index.in_(from_col_df['col_index']))
        to_col_df = to_col_df.rename({'id': 'col_id', 'index': 'col_index'}, axis=1)

        # Find cells and merge with cols
        from_cell = await self._retrieve_cell_df(session, sheet_id, self.cell_model.col_id.in_(from_col_df['col_id']))
        from_cell = pd.merge(from_cell, from_col_df, on=['col_id', 'sheet_id'])

        to_cell = await self._retrieve_cell_df(session, sheet_id, self.cell_model.col_id.in_(to_col_df['col_id']))
        to_cell = pd.merge(to_cell, to_col_df, on='col_id')

        # Create table with data and update cells
        table = pd.merge(
            from_cell[['value', 'dtype', 'row_id', 'col_index', 'sheet_id']],
            to_cell[['row_id', 'col_id', 'col_index', 'sheet_id']],
            on=['col_index', 'row_id', 'sheet_id'])

        await self._update_cells_with_bindparams(session, table, {'value': 'value', 'dtype': 'dtype'})

    async def _copy_cols_one_to_many(self, session: AsyncSession, sheet_id: core_types.Id_,
                                     copy_from: list[entities.CopySindex], copy_to: list[entities.CopySindex]) -> None:
        records = [{'col_id': x.id, 'col_index': x.index, 'sheet_id': x.sheet_id} for x in copy_to]
        to_row_df = pd.DataFrame(records)

        from_cell_df = await self._retrieve_cell_df(session, sheet_id, self.cell_model.col_id == copy_from[0].id)
        to_cell_df = await self._retrieve_cell_df(session, sheet_id, self.cell_model.col_id.in_(to_row_df['col_id']))
        table = pd.merge(
            from_cell_df[['value', 'dtype', 'row_id', 'sheet_id']],
            to_cell_df[['row_id', 'col_id', 'sheet_id']],
            on=['row_id', 'sheet_id']
        )
        await self._update_cells_with_bindparams(session, table, {'value': 'value', 'dtype': 'dtype'})

    async def _retrieve_row_df(self, session: AsyncSession, sheet_id: core_types.Id_, *args) -> pd.DataFrame:
        stmt = (
            select(self.row_model.__table__.c.id,
                   self.row_model.__table__.c.index, )
            .where(self.row_model.sheet_id == sheet_id, *args)
        )
        result = await session.execute(stmt)
        sindex_df = pd.DataFrame(result.fetchall(), columns=['id', 'index'])
        return sindex_df

    async def _retrieve_col_df(self, session: AsyncSession, sheet_id: core_types.Id_, *args) -> pd.DataFrame:
        stmt = (
            select(self.col_model.__table__.c.id,
                   self.col_model.__table__.c.index, )
            .where(self.col_model.sheet_id == sheet_id, *args)
        )
        result = await session.execute(stmt)
        sindex_df = pd.DataFrame(result.fetchall(), columns=['id', 'index'])
        return sindex_df

    async def _retrieve_cell_df(self, session: AsyncSession, sheet_id: core_types.Id_, *args):
        stmt = (
            select(self.cell_model.__table__.c.value,
                   self.cell_model.__table__.c.dtype,
                   self.cell_model.__table__.c.row_id,
                   self.cell_model.__table__.c.col_id,
                   self.cell_model.__table__.c.sheet_id, )
            .where(self.cell_model.sheet_id == sheet_id, *args)
        )
        result = await session.execute(stmt)
        cell_df = pd.DataFrame(result.fetchall(), columns=['value', 'dtype', 'row_id', 'col_id', 'sheet_id'])
        return cell_df

    async def _update_cells_with_bindparams(self, session: AsyncSession, cell_df: pd.DataFrame,
                                            bindparams: dict) -> None:
        if len(cell_df) == 0:
            raise ValueError('cell_df must have at least one row')
        if 'sheet_id' not in cell_df.columns:
            raise IndexError("'sheet_id' not in cell_df.columns")
        if 'row_id' not in cell_df.columns:
            raise IndexError("'row_id' not in cell_df.columns:")
        if 'col_id' not in cell_df.columns:
            raise IndexError("'col_id' not in cell_df.columns:")

        for key in bindparams.keys():
            if key not in cell_df.columns:
                raise IndexError(f'{key} not in {cell_df.columns}')

        cell_df.columns = [f'__{col}__' for col in cell_df.columns]
        to_update = {key: bindparam(f'__{value}__') for key, value in bindparams.items()}

        stmt = (
            self.cell_model.__table__.update()
            .where(
                ~self.cell_model.is_readonly,
                self.cell_model.sheet_id == bindparam('__sheet_id__'),
                self.cell_model.row_id == bindparam('__row_id__'),
                self.cell_model.col_id == bindparam('__col_id__'),
            )
            .values(to_update)
        )
        values = cell_df.to_dict(orient='records')
        _ = await session.execute(stmt, values)
