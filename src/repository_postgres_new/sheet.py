import numpy as np
import pandas as pd
from sqlalchemy import insert, select, func, bindparam, update, delete, Integer, Boolean, ForeignKey, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from src import core_types
from src.sheet import entities, schema
from src.sheet.repository import SheetRepo
from .normalizer import Normalizer, Denormalizer
from .base import BasePostgres, Model, BaseModel


class SheetModel(BaseModel):
    __tablename__ = "sheet"

    def to_entity(self, **kwargs) -> entities.Sheet:
        raise NotImplemented


class RowModel(BaseModel):
    __tablename__ = "sheet_row"
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_readonly: Mapped[int] = mapped_column(Boolean, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(SheetModel.id, ondelete='CASCADE'), nullable=False,
                                          index=True)


class ColModel(BaseModel):
    __tablename__ = "sheet_col"
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    is_readonly: Mapped[int] = mapped_column(Boolean, nullable=False)
    is_freeze: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    index: Mapped[int] = mapped_column(Integer, nullable=False)
    scroll_pos: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(SheetModel.id, ondelete='CASCADE'), nullable=False,
                                          index=True)


class CellModel(BaseModel):
    __tablename__ = "sheet_cell"
    value: Mapped[str] = mapped_column(String(1000), nullable=True, index=True)
    dtype: Mapped[str] = mapped_column(String(30), nullable=False)
    is_readonly: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_filtred: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_index: Mapped[bool] = mapped_column(Boolean, nullable=False)
    color: Mapped[str] = mapped_column(String(16), nullable=True)
    text_align: Mapped[str] = mapped_column(String(8), default='left')
    row_id: Mapped[int] = mapped_column(Integer, ForeignKey(RowModel.id, ondelete='CASCADE'), nullable=False,
                                        index=True)
    col_id: Mapped[int] = mapped_column(Integer, ForeignKey(ColModel.id, ondelete='CASCADE'), nullable=False,
                                        index=True)
    sheet_id: Mapped[int] = mapped_column(Integer, ForeignKey(SheetModel.id, ondelete='CASCADE'), nullable=False,
                                          index=True)


class SheetSindex(BasePostgres):
    model: Model = NotImplemented

    async def create_many(self, data: list[core_types.DTO]) -> list[core_types.Id_]:
        session = self._session
        data = self._parse_dto(data)
        stmt = insert(self.model).returning(self.model.id)
        result = await session.execute(stmt, data)
        ids = list(result.scalars())
        return ids

    async def delete_many(self, filter_by: dict) -> None:
        sheet_id: core_types.Id_ = filter_by['sheet_id']
        sindex_ids: list[core_types.Id_] = filter_by['row_ids']
        stmt = (
            delete(self.model)
            .where(self.model.sheet_id == sheet_id,
                   self.model.id.in_(sindex_ids),
                   ~self.model.is_freeze,
                   ~self.model.is_readonly,
                   )
        )
        _ = await self._session.execute(stmt)
        await self._update_scroll_pos_and_indexes(sheet_id)

    async def update_many(self, data: core_types.DTO, filter_by: dict) -> None:
        data: dict = self._parse_dto(data)
        filters = self._parse_filters(filter_by)
        stmt = update(self.model).where(*filters).values(**data)
        await self._session.execute(stmt)
        await self._update_scroll_pos_and_indexes(filter_by['sheet_id'])

    async def _update_scroll_pos_and_indexes(self, sheet_id: core_types.Id_) -> None:
        # Get data
        filter_by = {"sheet_id": sheet_id}
        order_by = 'index'
        sindexes = await super().get_many_as_frame(filter_by, order_by)
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
        _ = await self._session.execute(stmt, values)


class SheetRow(SheetSindex):
    model = RowModel


class SheetCol(SheetSindex):
    model = ColModel


class SheetCell(BasePostgres):
    model = CellModel

    async def create_many(self, data: list[core_types.DTO]) -> None:
        session = self._session
        data = self._parse_dto(data)
        stmt = insert(self.model).returning(self.model)
        _ = await session.execute(stmt, data)

    async def update_one(self, sheet_id: core_types.Id_, data: schema.PartialUpdateCellSchema) -> None:
        data = {key: value for key, value in data.dict().items() if value is not None}
        filter_by = {'id': data.pop('id')} | {'sheet_id': sheet_id, 'is_readonly': False}
        _ = await super().update_one(data, filter_by)

    async def update_many(self, sheet_id: core_types.Id_, data: list[entities.Cell]) -> None:
        values = []
        for cell in data:
            c = cell.copy()
            c['cell_id'] = c.pop('id')
            c['cell_value'] = c.pop('value')
            c['cell_dtype'] = c.pop('dtype')
            values.append(c)

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
        _ = await self._session.execute(stmt, values)


class SheetFilter:
    __row_model = RowModel
    __cell_model = CellModel

    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_col_filter(self, data: schema.ColFilterRetrieveSchema) -> entities.ColFilter:
        stmt = (
            select(self.__cell_model.value, self.__cell_model.dtype, self.__cell_model.is_filtred)
            .distinct()
            .filter_by(sheet_id=data.sheet_id, col_id=data.col_id, is_index=False)
            .order_by(self.__cell_model.value)
        )
        items = await self._session.execute(stmt)
        columns = [self.__cell_model.value.key, self.__cell_model.dtype.key, self.__cell_model.is_filtred.key]
        items = pd.DataFrame.from_records(items.fetchall(), columns=columns).to_dict(orient='records')
        col_filter = entities.ColFilter(col_id=data.col_id, sheet_id=data.sheet_id,
                                        items=[entities.FilterItem(**x) for x in items])
        return col_filter

    async def update_col_filter(self, data: entities.ColFilter) -> None:
        await self._update_filtred_flag_in_cells(data)
        await self._update_filtred_flag_and_scroll_pos_in_rows(sheet_id=data.sheet_id)

    async def clear_all_filters(self, sheet_id: core_types.Id_) -> None:
        stmt = update(self.__cell_model).where(self.__cell_model.sheet_id == sheet_id)
        await self._session.execute(stmt, {"is_filtred": True})
        await self._update_filtred_flag_and_scroll_pos_in_rows(sheet_id=sheet_id)

    async def _retrieve_total_rows(self, sheet_id: core_types.Id_) -> pd.DataFrame:
        stmt = (
            select(
                self.__row_model.__table__.c.id,
                self.__row_model.__table__.c.size,
                self.__row_model.__table__.c.is_freeze,
                self.__row_model.__table__.c.scroll_pos, )
            .filter_by(sheet_id=sheet_id)
            .order_by(self.__row_model.index)
        )
        rows = await self._session.execute(stmt)
        rows = pd.DataFrame.from_records(rows.fetchall(), columns=['row_id', 'size', 'is_freeze', 'scroll_pos'])
        return rows

    async def _retrieve_filtred_rows(self, sheet_id: core_types.Id_) -> pd.DataFrame:
        stmt = (
            select(self.__cell_model.row_id, func.count(self.__cell_model.is_filtred)
                   .filter(~self.__cell_model.is_filtred))
            .group_by(self.__cell_model.row_id)
            .filter(self.__cell_model.sheet_id == sheet_id)
        )
        result = await self._session.execute(stmt)
        filtred_rows = pd.DataFrame.from_records(result.fetchall(), columns=['row_id', 'is_filtred'])
        filtred_rows['is_filtred'] = ~filtred_rows['is_filtred'].astype(bool)
        return filtred_rows

    async def _update_filtred_flag_and_scroll_pos_in_rows(self, sheet_id: core_types.Id_) -> None:
        rows = await self._retrieve_total_rows(sheet_id)
        filtred_rows = await self._retrieve_filtred_rows(sheet_id)

        if len(rows) != len(filtred_rows):
            raise Exception

        rows = pd.merge(rows, filtred_rows, on='row_id', how='inner')

        rows.loc[~rows['is_filtred'] | rows['is_freeze'], 'size'] = 0
        rows['scroll_pos'] = rows['size'].cumsum().shift(1).fillna(0)
        rows.loc[rows['is_freeze'], 'scroll_pos'] = -1

        values: list[dict] = rows[['row_id', 'is_filtred', 'scroll_pos']].to_dict(orient='records')
        stmt = (
            self.__row_model.__table__.update()
            .where(self.__row_model.__table__.c.id == bindparam('row_id'))
            .values({"scroll_pos": bindparam("scroll_pos"), "is_filtred": bindparam("is_filtred")})
        )
        _ = await self._session.execute(stmt, values)

    async def _update_filtred_flag_in_cells(self, data: entities.ColFilter) -> None:
        # Convert input data because "value" is reserved word in bindparam function
        items = [{
            'cell_value': x.value,
            'is_filtred': x.is_filtred,
            'dtype': x.dtype,
        } for x in data.items]

        stmt = (
            self.__cell_model.__table__.update()
            .where(self.__cell_model.__table__.c.value == bindparam('cell_value'),
                   self.__cell_model.col_id == data.col_id)
            .values({"is_filtred": bindparam("is_filtred")})
        )
        _ = await self._session.execute(stmt, items)


class SheetSorter:
    __row_model = RowModel
    __cell_model = CellModel

    def __init__(self, session: AsyncSession):
        self._session = session

    async def update_col_sorter(self, data: entities.ColSorter) -> None:
        sorted_rows = await self._retrieve_sorted_rows(data.sheet_id, data.col_id, data.ascending)
        filtred_rows = await self._retrieve_filtred_rows(data.sheet_id)
        await self._update_row_index_and_scroll_pos(sorted_rows, filtred_rows)

    async def _update_row_index_and_scroll_pos(self, sorted_rows: pd.DataFrame, filtred_rows: pd.DataFrame) -> None:
        merged = pd.merge(filtred_rows, sorted_rows, on='row_id', how='left').sort_values('row_index')
        merged.loc[merged['is_index'], 'size'] = 0
        merged['scroll_pos'] = merged['size'].cumsum().shift(1).fillna(0)
        merged.loc[merged['is_index'], 'scroll_pos'] = -1

        values = merged[['row_id', 'row_index', 'scroll_pos']].to_dict(orient='records')

        stmt = (
            self.__row_model.__table__.update()
            .where(self.__row_model.__table__.c.id == bindparam('row_id'))
            .values({"scroll_pos": bindparam("scroll_pos"), "index": bindparam("row_index")})
        )
        _ = await self._session.execute(stmt, values)

    async def _retrieve_filtred_rows(self, sheet_id: core_types.Id_) -> pd.DataFrame:
        stmt = (
            select(
                self.__row_model.__table__.c.id,
                self.__row_model.__table__.c.size
            )
            .where(
                self.__row_model.sheet_id == sheet_id,
                self.__row_model.__table__.c.is_filtred)
        )
        rows = await self._session.execute(stmt)
        rows = pd.DataFrame.from_records(rows.fetchall(), columns=['row_id', 'size'])
        return rows

    async def _retrieve_sorted_rows(self, sheet_id: core_types.Id_, col_id: core_types.Id_, asc: bool) -> pd.DataFrame:
        # Retrieving
        stmt = (
            select(
                self.__cell_model.__table__.c.row_id,
                self.__cell_model.__table__.c.value,
                self.__cell_model.__table__.c.is_index)
            .where(
                self.__cell_model.sheet_id == sheet_id,
                self.__cell_model.col_id == col_id, )
        )
        result = await self._session.execute(stmt)

        # Sorting
        result = pd.DataFrame.from_records(result.fetchall(), columns=['row_id', 'cell_value', 'is_index'])
        freeze_part = result.loc[result['is_index']]
        free_part = result.loc[~result['is_index']].sort_values('cell_value', ascending=asc)
        sorted_df = pd.concat([freeze_part, free_part], ignore_index=True)
        sorted_df['row_index'] = range(len(sorted_df))
        return sorted_df


class SheetCrud(BasePostgres):
    model = SheetModel

    def __init__(self, session: AsyncSession):
        super().__init__(session)
        self.__sheet_cell = SheetCell(session)
        self.__sheet_row = SheetRow(session)
        self.__sheet_col = SheetCol(session)
        self.normalizer = Normalizer
        self.denormalizer = Denormalizer

    async def create_one(self, data: schema.SheetCreateSchema) -> core_types.Id_:
        sheet: SheetModel = await super().create_one({})
        await self._create_rows_cols_and_cells(sheet.id, data)
        return sheet.id

    async def get_one(self, data: schema.SheetRetrieveSchema) -> entities.Sheet:
        filter_by = {"sheet_id": data.sheet_id, "is_filtred": True, }
        order_by = 'index'
        rows = await self.__sheet_row.get_many_as_frame(filter_by, order_by)
        cols = await self.__sheet_col.get_many_as_frame(filter_by, order_by)
        cells = await self.__sheet_cell.get_many_as_frame(filter_by)
        return self._merge_into_sheet_entity(data.sheet_id, rows, cols, cells)

    async def get_one_as_frame(self, filter_by: dict) -> pd.DataFrame:
        cells = await self.__sheet_cell.get_many_as_frame(filter_by)
        rows = await self.__sheet_row.get_many_as_frame(filter_by)
        cols = await self.__sheet_col.get_many_as_frame(filter_by)
        denormalizer = self.denormalizer(rows, cols, cells)
        denormalizer.denormalize()
        df = denormalizer.get_denormalized()
        return df

    async def overwrite_one(self, sheet_id: core_types.Id_, data: entities.SheetCreate) -> None:
        # Delete old data
        filter_by = {"sheet_id": sheet_id}
        await self.__sheet_row.delete_many(filter_by)
        await self.__sheet_col.delete_many(filter_by)
        await self.__sheet_cell.delete_many(filter_by)
        # Create new data
        await self._create_rows_cols_and_cells(sheet_id, data)

    async def _create_rows_cols_and_cells(self, sheet_id: core_types.Id_, data: entities.SheetCreate) -> None:
        # Create row, col and cell data from denormalized dataframe
        normalizer = self.normalizer(**data.dict())
        normalizer.normalize()

        # Create sindexes, save created ids for following create cells
        rows = normalizer.get_normalized_rows().assign(sheet_id=sheet_id).to_dict(orient='records')
        cols = normalizer.get_normalized_cols().assign(sheet_id=sheet_id).to_dict(orient='records')

        row_ids = await self.__sheet_row.create_many(rows)
        col_ids = await self.__sheet_col.create_many(cols)

        # Create cells
        repeated_row_ids = np.repeat(row_ids, len(col_ids))
        repeated_col_ids = col_ids * len(row_ids)
        cells = normalizer.get_normalized_cells().assign(
            sheet_id=sheet_id, row_id=repeated_row_ids, col_id=repeated_col_ids).to_dict(orient='records')
        _ = await self.__sheet_cell.create_many(cells)

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


class SheetRepoPostgres(SheetRepo):

    def __init__(self, session: AsyncSession):
        self.__sheet_crud = SheetCrud(session)
        self.__sheet_cell = SheetCell(session)
        self.__sheet_row = SheetRow(session)
        self.__sheet_col = SheetCol(session)
        self.__sheet_filter = SheetFilter(session)
        self.__sheet_sorter = SheetSorter(session)

    async def create_one(self, data: schema.SheetCreateSchema) -> core_types.Id_:
        return await self.__sheet_crud.create_one(data)

    async def get_one(self, data: schema.SheetRetrieveSchema) -> entities.Sheet:
        return await self.__sheet_crud.get_one(data)

    async def get_one_as_frame(self, sheet_id: core_types.Id_) -> pd.DataFrame:
        filter_by = {"sheet_id": sheet_id}
        return await self.__sheet_crud.get_one_as_frame(filter_by)

    async def overwrite_one(self, sheet_id: core_types.Id_, data: entities.SheetCreate) -> None:
        await self.__sheet_crud.overwrite_one(sheet_id, data)

    async def delete_many(self, filter_by: dict) -> None:
        await self.__sheet_crud.delete_many(filter_by)

    async def delete_one(self, filter_by: dict) -> None:
        await self.__sheet_crud.delete_one(filter_by)

    async def get_scroll_size(self, sheet_id: core_types.Id_) -> entities.ScrollSize:
        raise NotImplemented

    async def update_col_size(self, sheet_id: core_types.Id_, data: schema.UpdateSindexSizeSchema) -> None:
        filter_by = {'sheet_id': sheet_id, 'id': data.sindex_id}
        data = {"size": data.new_size}
        await self.__sheet_col.update_many(data, filter_by)

    async def update_cell_one(self, sheet_id: core_types.Id_, data: schema.PartialUpdateCellSchema) -> None:
        await self.__sheet_cell.update_one(sheet_id, data)

    async def update_cell_many(self, sheet_id: core_types.Id_, data: list[entities.Cell]) -> None:
        await self.__sheet_cell.update_many(sheet_id, data)

    async def delete_row_many(self, sheet_id: core_types.Id_, row_ids: list[core_types.Id_]) -> None:
        filter_by = {"sheet_id": sheet_id, "row_ids": row_ids, }
        await self.__sheet_row.delete_many(filter_by)

    async def get_col_filter(self, data: schema.ColFilterRetrieveSchema) -> entities.ColFilter:
        return await self.__sheet_filter.get_col_filter(data)

    async def update_col_filter(self, data: entities.ColFilter) -> None:
        await self.__sheet_filter.update_col_filter(data)

    async def update_col_sorter(self, data: entities.ColSorter) -> None:
        await self.__sheet_sorter.update_col_sorter(data)

    async def clear_all_filters(self, sheet_id: core_types.Id_) -> None:
        await self.__sheet_filter.clear_all_filters(sheet_id)
