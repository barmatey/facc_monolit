import numpy as np
import pandas as pd

from .entities import SindexCreate
from .entities import CellCreate
from .enums import CellDtype


class Normalizer:

    def __init__(self, df: pd.DataFrame, drop_index: bool, drop_columns: bool):
        self.df = df.copy()
        self.drop_index = drop_index
        self.drop_columns = drop_columns

        self.rows: list[SindexCreate] = []
        self.cols: list[SindexCreate] = []
        self.cells: list[CellCreate] = []

    def get_normalized_rows(self) -> list[SindexCreate]:
        return self.rows

    def get_normalized_cols(self) -> list[SindexCreate]:
        return self.cols

    def get_normalized_cells(self) -> list[CellCreate]:
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
            cell_df: pd.DataFrame) -> list[CellCreate]:

        col_is_freeze = col_df['is_freeze'].tolist() * len(row_df.index)
        row_is_freeze = np.repeat(row_df['is_freeze'].tolist(), len(col_df.index))
        readonly_flag = np.where(
            np.logical_or(row_is_freeze, col_is_freeze), True, False
        )

        def get_dtype(value):
            value_type = type(value)
            if value_type is int or value_type is float:
                return CellDtype.NUMBER.value
            if value_type is bool:
                return CellDtype.BOOLEAN.value
            return CellDtype.TEXT.value

        flatten = pd.DataFrame(cell_df.stack().values, columns=['value'])
        flatten['is_index'] = readonly_flag
        flatten['is_readonly'] = readonly_flag
        flatten['is_filtred'] = True
        flatten['dtype'] = flatten['value'].apply(get_dtype)
        flatten['color'] = np.where(flatten['is_index'], '#f8fafd', 'white')

        cells: list[CellCreate] = flatten.to_dict(orient='records')
        return cells
