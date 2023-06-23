import typing

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import ForeignKey, Integer, Boolean, String, bindparam
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


class FiltredRow(typing.TypedDict):
    row_id: core_types.Id_
    is_filtred: bool


class SindexRepo(BaseRepo):
    model: Row | Col = None

    async def retrieve_scroll_size_with_session(self, session: AsyncSession,
                                                sheet_id: core_types.Id_) -> SindexScrollSize:
        count = await session.scalar(
            select(func.count())
            .where(self.model.sheet_id == sheet_id)
        )

        stmt = (
            select(func.max(self.model.scroll_pos))
            .where(self.model.sheet_id == sheet_id)
        )
        scroll_size = await session.scalar(stmt)

        sindex_scroll_size = SindexScrollSize(
            count_sindexes=count,
            scroll_size=scroll_size,
        )
        return sindex_scroll_size

    async def update_filtred_flag_with_session(self, session: AsyncSession, items: list[FiltredRow]) -> None:
        stmt = (
            self.model.__table__.update()
            .where(self.model.__table__.c.id == bindparam('row_id'))
            .values({"is_filtred": bindparam("is_filtred")})
        )
        _ = await session.execute(stmt, items)

    async def calculate_scroll_pos_with_session(self, session: AsyncSession, sheet_id: core_types.Id_) -> None:
        # Get rows, drop unfiltred, sort by index, calculate scroll_pos
        rows = await self.retrieve_bulk_as_records_with_session(session, filter_={"sheet_id": sheet_id})
        rows = pd.DataFrame.from_records(rows, columns=self.model.get_columns())
        rows = rows.loc[rows['is_filtred']]
        rows = rows.sort_values('index')
        rows.loc[rows['is_freeze'], 'scroll_pos'] = -1
        rows.loc[~rows['is_freeze'], 'scroll_pos'] = rows.loc[~rows['is_freeze'], 'size'].shift(1).cumsum()
        rows['scroll_pos'] = rows['scroll_pos'].fillna(0)

        # Update rows
        values: list[FiltredRow] = rows[['id', 'scroll_pos']].rename({'id': 'row_id'}, axis=1).to_dict(orient='records')
        stmt = (
            self.model.__table__.update()
            .where(self.model.__table__.c.id == bindparam('row_id'))
            .values({"scroll_pos": bindparam("scroll_pos")})
        )
        _ = await session.execute(stmt, values)


class RowRepo(SindexRepo):
    model = Row


class ColRepo(SindexRepo):
    model = Col


class CellRepo(BaseRepo):
    model = Cell


class SheetRepo(BaseRepo):
    model = Sheet
    row_repo = RowRepo
    col_repo = ColRepo
    cell_repo = CellRepo
    normalizer = Normalizer
    denormalizer = Denormalizer

    async def create(self, data: entities.SheetCreate) -> core_types.Id_:
        async with db.get_async_session() as session:
            sheet_id = await self.create_with_session(session, data)
            await session.commit()
            return sheet_id

    async def create_with_session(self, session: AsyncSession, data: entities.SheetCreate) -> core_types.Id_:
        sheet_id = await super().create_with_session(session, {})

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
            return await self.retrieve_as_sheet_with_session(session, data)

    async def retrieve_as_sheet_with_session(self, session: AsyncSession,
                                             data: entities.SheetRetrieve) -> entities.Sheet:
        # Find rows
        stmt = (
            select(self.row_repo.model.__table__)
            .where(
                self.row_repo.model.__table__.c.sheet_id == data.sheet_id,
                self.row_repo.model.__table__.c.is_filtred,
                or_(
                    and_(self.row_repo.model.__table__.c.scroll_pos >= data.from_scroll,
                         self.row_repo.model.__table__.c.scroll_pos < data.to_scroll),
                    self.row_repo.model.__table__.c.is_freeze,
                )
            )
            .order_by(self.row_repo.model.__table__.c.scroll_pos)
        )
        result = await session.execute(stmt)
        rows = pd.DataFrame.from_records(result.fetchall(), columns=Row.get_columns())

        # Find Cols
        stmt = (
            select(self.col_repo.model.__table__)
            .where(self.col_repo.model.__table__.c.sheet_id == data.sheet_id)
            .order_by(self.col_repo.model.__table__.c.scroll_pos)
        )
        result = await session.execute(stmt)
        cols = pd.DataFrame.from_records(result.fetchall(), columns=Col.get_columns())

        # Find cells
        stmt = (
            select(self.cell_repo.model.__table__)
            .where(self.cell_repo.model.__table__.c.sheet_id == data.sheet_id,
                   self.cell_repo.model.__table__.c.row_id.in_(rows['id'].tolist()),
                   self.cell_repo.model.__table__.c.col_id.in_(cols['id'].tolist()),
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

    async def retrieve_as_dataframe(self, id_: core_types.Id_) -> pd.DataFrame:
        async with db.get_async_session() as session:
            df = await self.retrieve_as_dataframe_with_session(session, id_)
            return df

    async def retrieve_as_dataframe_with_session(self, session: AsyncSession, id_: core_types.Id_) -> pd.DataFrame:
        cells: list[tuple] = await self.cell_repo().retrieve_bulk_as_records_with_session(session, {"sheet_id": id_})
        rows: list[tuple] = await self.row_repo().retrieve_bulk_as_records_with_session(session, {"sheet_id": id_})
        cols: list[tuple] = await self.col_repo().retrieve_bulk_as_records_with_session(session, {"sheet_id": id_})

        rows: pd.DataFrame = pd.DataFrame.from_records(rows, columns=Row.get_columns())
        cols: pd.DataFrame = pd.DataFrame.from_records(cols, columns=Col.get_columns())
        cells: pd.DataFrame = pd.DataFrame.from_records(cells, columns=Cell.get_columns())

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
    col_model = Col
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

    async def _update_filtred_flag_in_cells_with_session(self, session: AsyncSession, data: entities.ColFilter) -> None:
        # Convert input data because "value" is reserved word in bindparam function
        items = pd.DataFrame.from_dict(data['items']).rename({'value': 'cell_value'}, axis=1).to_dict(
            orient='records')

        stmt = (
            self.cell_model.__table__.update()
            .where(self.cell_model.__table__.c.value == bindparam('cell_value'),
                   self.cell_model.col_id == data['col_id'])
            .values({"is_filtred": bindparam("is_filtred")})
        )
        _ = await session.execute(stmt, items)

    async def _retrieve_total_sheet_rows_with_session(self, session: AsyncSession,
                                                      sheet_id: core_types.Id_) -> pd.DataFrame:
        stmt = (
            select(
                self.row_model.__table__.c.id,
                self.row_model.__table__.c.size,
                self.row_model.__table__.c.is_freeze,
                self.row_model.__table__.c.scroll_pos)
            .filter_by(sheet_id=sheet_id)
            .order_by(self.row_model.__table__.c.index)
        )
        rows = await session.execute(stmt)
        rows = pd.DataFrame.from_records(rows.fetchall(), columns=['row_id', 'size', 'is_freeze', 'scroll_pos'])
        return rows

    async def _find_filtred_rows_with_session(self, session: AsyncSession,
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
        rows = await self._retrieve_total_sheet_rows_with_session(session, sheet_id)
        filtred_rows = await self._find_filtred_rows_with_session(session, sheet_id)

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
