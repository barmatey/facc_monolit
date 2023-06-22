import typing

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import ForeignKey, Integer, Boolean, String, update, bindparam
from sqlalchemy import func, select, or_, and_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from src import helpers
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

    @helpers.async_timeit
    async def update_filtred_flag_with_session(self, session: AsyncSession, items: list[FiltredRow]) -> None:
        stmt = (
            update(self.model.__table__)
            .where(self.model.__table__.c.id == bindparam('row_id'))
            .values({"is_filtred": bindparam("is_filtred")})
        )
        _ = await session.execute(stmt, items)

    @helpers.async_timeit
    async def calculate_scroll_pos_with_session(self, session: AsyncSession, sheet_id: core_types.Id_) -> None:
        # Get rows, drop unfiltred, sort by index, calculate scroll_pos
        rows = await self.retrieve_bulk_as_records_with_session(session, filter_={"sheet_id": sheet_id})
        rows = pd.DataFrame.from_records(rows, columns=self.model.get_columns())
        rows = rows.loc[rows['is_filtred']]
        rows = rows.sort_values('index')
        rows.loc[rows['is_freeze'], 'scroll_pos'] = -1
        rows.loc[~rows['is_freeze'], 'scroll_pos'] = rows.loc[~rows['is_freeze'], 'size'].shift(1).cumsum()
        rows['scroll_pos'] = rows['scroll_pos'].fillna(0)
        rows = rows[['id', 'scroll_pos']].rename({'id': 'row_id'}, axis=1).to_dict(orient='records')

        # Update rows
        stmt = (
            update(self.model.__table__)
            .where(self.model.__table__.c.id == bindparam('row_id'))
            .values({"scroll_pos": bindparam("scroll_pos")})
        )
        _ = await session.execute(stmt, rows)


class RowRepo(SindexRepo):
    model = Row


class ColRepo(SindexRepo):
    model = Col


class CellRepo(BaseRepo):
    model = Cell

    async def retrieve_filter_items_with_session(self, session: AsyncSession,
                                                 filter_: dict) -> list[entities.FilterItem]:
        items = await session.execute(
            select(self.model.value, self.model.dtype, self.model.is_filtred)
            .distinct()
            .filter_by(**filter_)
            .order_by(self.model.value)
        )

        items = pd.DataFrame.from_records(items.fetchall(), columns=[
            self.model.value.key, self.model.dtype.key, self.model.is_filtred.key]).to_dict(orient='records')
        return items

    async def retrieve_filter_items(self, filter_: dict) -> list[entities.FilterItem]:
        async with db.get_async_session() as session:
            return await self.retrieve_filter_items_with_session(session, filter_)

    @helpers.async_timeit
    async def find_filtred_rows_with_session(self, session: AsyncSession, data: entities.ColFilter) -> list[FiltredRow]:
        stmt = (
            select(self.model.row_id, func.count(self.model.is_filtred).filter(~self.model.is_filtred))
            .group_by(self.model.row_id)
            .filter(self.model.sheet_id == data['sheet_id'])
        )
        result = await session.execute(stmt)
        filtred_rows = pd.DataFrame.from_records(result.fetchall(), columns=['row_id', 'is_filtred'])
        filtred_rows['is_filtred'] = ~filtred_rows['is_filtred'].astype(bool)
        filtred_rows = filtred_rows.to_dict(orient='records')
        return filtred_rows

    @helpers.async_timeit
    async def update_filtred_flag_with_session(self, session: AsyncSession, data: entities.ColFilter) -> None:
        # Convert input data because "value" is reserved word in bindparam function
        items = pd.DataFrame.from_dict(data['items']).rename({'value': 'cell_value'}, axis=1).to_dict(
            orient='records')
        stmt = (
            update(self.model.__table__)
            .where(self.model.__table__.c.value == bindparam('cell_value'), self.model.col_id == data['col_id'])
            .values({"is_filtred": bindparam("is_filtred")})
        )
        _ = await session.execute(stmt, items)


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

    @helpers.async_timeit
    async def update_col_filter(self, data: entities.ColFilter) -> None:
        async with db.get_async_session() as session:
            await self.cell_repo().update_filtred_flag_with_session(session, data)
            filtred_rows = await self.cell_repo().find_filtred_rows_with_session(session, data)
            await self.row_repo().update_filtred_flag_with_session(session, filtred_rows)
            await self.row_repo().calculate_scroll_pos_with_session(session, sheet_id=data['sheet_id'])
            await session.commit()
