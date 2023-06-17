import numpy as np
from loguru import logger
import pandas as pd
from sqlalchemy import ForeignKey, MetaData, Table, Column, Integer, Boolean, String

from .. import core_types
from ..sheet import entities, enums
from .base import BaseRepo

metadata = MetaData()

Sheet = Table(
    'sheet',
    metadata,
    Column("id", Integer, primary_key=True),
)

Row = Table(
    'sheet_row',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("size", Integer, nullable=False),
    Column("is_freeze", Boolean, nullable=False),
    Column("is_filtred", Boolean, nullable=False),
    Column("index", Integer, nullable=False),
    Column("scroll_pos", Integer, nullable=False),
    Column("sheet_id", Integer, ForeignKey(Sheet.c.id, ondelete='CASCADE'), nullable=False),
)

Col = Table(
    'sheet_col',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("size", Integer, nullable=False),
    Column("is_freeze", Boolean, nullable=False),
    Column("is_filtred", Boolean, nullable=False),
    Column("index", Integer, nullable=False),
    Column("scroll_pos", Integer, nullable=False),
    Column("sheet_id", Integer, ForeignKey(Sheet.c.id, ondelete='CASCADE'), nullable=False),
)

Cell = Table(
    'sheet_cell',
    metadata,
    Column("id", Integer, primary_key=True),
    Column("value", String(1000), nullable=True),
    Column("dtype", String(30), nullable=False),
    Column("is_readonly", Boolean, nullable=False),
    Column("is_filtred", Boolean, nullable=False),
    Column("sheet_id", Integer, ForeignKey(Sheet.c.id, ondelete='CASCADE'), nullable=False),
)


class _Normalizer:

    def __init__(self, df: pd.DataFrame, drop_index: bool, drop_columns: bool):
        self.df = df.copy()
        self.drop_index = drop_index
        self.drop_columns = drop_columns

        self.rows: list[entities.SindexCreate] = []
        self.cols: list[entities.SindexCreate] = []
        self.cells: list[entities.CellCreate] = []

    def get_normalized_rows(self) -> list[entities.SindexCreate]:
        return self.rows

    def get_normalized_cols(self) -> list[entities.SindexCreate]:
        return self.cols

    def get_normalized_cells(self) -> list[entities.CellCreate]:
        return self.cells

    def normalize(self):
        table = self.df.copy()
        table = table.reset_index(drop=self.drop_index)

        table = table.transpose().reset_index(drop=self.drop_columns).transpose()
        table = table.reset_index(drop=True)

        vertical_difference = len(table) - len(self.df)
        horizontal_difference = len(table.columns) - len(self.df.columns)

        height = 24
        rows = pd.DataFrame(
            [],
            index=range(0, len(table.index)),
            columns=['size', 'is_freeze', "is_filtred"],
        )
        rows['size'] = height
        rows['is_freeze'] = False
        rows['is_filtred'] = True

        width = 120
        cols = pd.DataFrame(
            [],
            index=range(0, len(table.columns)),
            columns=['size', 'is_freeze', "is_filtred"],
        )
        cols['size'] = width
        cols['is_freeze'] = False
        cols['is_filtred'] = True

        if vertical_difference > 0:
            rows.iloc[0:vertical_difference, 1] = True

        if horizontal_difference > 0:
            cols.iloc[0:horizontal_difference, 1] = True

        cells = self.to_table_cells(rows, cols, table)
        rows = rows.to_dict(orient='records')
        cols = cols.to_dict(orient='records')

        self.rows = rows
        self.cols = cols
        self.cells = cells

    @staticmethod
    def to_table_cells(
            row_df: pd.DataFrame,
            col_df: pd.DataFrame,
            cell_df: pd.DataFrame) -> list[entities.CellCreate]:

        col_is_freeze = col_df['is_freeze'].tolist() * len(row_df.index)
        row_is_freeze = np.repeat(row_df['is_freeze'].tolist(), len(col_df.index))
        readonly_flag = np.where(
            np.logical_or(row_is_freeze, col_is_freeze), True, False
        )

        def get_dtype(value):
            value_type = type(value)
            if value_type is int or value_type is float:
                return enums.CellDtype.NUMBER.value
            if value_type is bool:
                return enums.CellDtype.BOOLEAN.value
            return enums.CellDtype.TEXT.value

        flatten = pd.DataFrame(cell_df.stack().values, columns=['value'])
        flatten['is_index'] = readonly_flag
        flatten['is_readonly'] = readonly_flag
        flatten['is_filtred'] = True
        flatten['dtype'] = flatten['value'].apply(get_dtype)
        flatten['color'] = np.where(flatten['is_index'], '#f8fafd', 'white')

        cells: list[entities.CellCreate] = flatten.to_dict(orient='records')
        return cells


class SindexRepo(BaseRepo):
    pass


class RowRepo(SindexRepo):
    table = Row


class ColRepo(SindexRepo):
    table = Col


class CellRepo(BaseRepo):
    table = Cell


class SheetRepo(BaseRepo):
    row_repo = RowRepo
    col_repo = ColRepo
    cell_repo = CellRepo
    normalizer = _Normalizer

    async def create_from_dataframe(self, df: pd.DataFrame, drop_index: bool, drop_columns: bool) -> core_types.Id_:
        normalizer = self.normalizer(df, drop_index, drop_columns)
        normalizer.normalize()
        rows: list[entities.SindexCreate] = normalizer.get_normalized_rows()
        cols: list[entities.SindexCreate] = normalizer.get_normalized_cols()
        cells: list[entities.CellCreate] = normalizer.get_normalized_cells()

        return 321
