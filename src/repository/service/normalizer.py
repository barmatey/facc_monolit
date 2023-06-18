import numpy as np
import pandas as pd

from ...sheet import enums


class Normalizer:

    def __init__(self, df: pd.DataFrame, drop_index: bool, drop_columns: bool):
        self.df = df.copy()
        self.drop_index = drop_index
        self.drop_columns = drop_columns

        self.rows: pd.DataFrame = pd.DataFrame([])
        self.cols: pd.DataFrame = pd.DataFrame([])
        self.cells: pd.DataFrame = pd.DataFrame([])

    def get_normalized_rows(self) -> pd.DataFrame:
        return self.rows

    def get_normalized_cols(self) -> pd.DataFrame:
        return self.cols

    def get_normalized_cells(self) -> pd.DataFrame:
        return self.cells

    def normalize(self):
        table = self.df.copy()
        table = table.reset_index(drop=self.drop_index)

        table = table.transpose().reset_index(drop=self.drop_columns).transpose()
        table = table.reset_index(drop=True)

        vertical_difference = len(table) - len(self.df)
        horizontal_difference = len(table.columns) - len(self.df.columns)

        rows: pd.DataFrame = self.create_sindex_df(len(table.index), vertical_difference, 24)
        cols: pd.DataFrame = self.create_sindex_df(len(table.columns), horizontal_difference, 120)
        cells: pd.DataFrame = self.create_cell_df(rows, cols, table)

        self.rows = rows
        self.cols = cols
        self.cells = cells

    @staticmethod
    def create_sindex_df(total_item_count: int, freeze_item_count: int, item_size: int) -> pd.DataFrame:
        sindex = pd.DataFrame(
            [],
            index=range(0, total_item_count),
            columns=['size', 'is_freeze', 'is_filtred', 'index', 'scroll_pos']
        )
        sindex['size'] = item_size
        sindex['is_freeze'] = False
        sindex['is_filtred'] = True
        sindex['index'] = sindex.index
        sindex['scroll_pos'] = sindex['size'].cumsum() - item_size * total_item_count
        sindex['scroll_pos'] = np.where(sindex['scroll_pos'] < 0, -1, sindex['scroll_pos'])

        if freeze_item_count > 0:
            sindex.iloc[0:freeze_item_count, 1] = True

        return sindex

    @staticmethod
    def create_cell_df(rows: pd.DataFrame, cols: pd.DataFrame, table: pd.DataFrame) -> pd.DataFrame:
        col_is_freeze = cols['is_freeze'].tolist() * len(rows.index)
        row_is_freeze = np.repeat(rows['is_freeze'].tolist(), len(cols.index))
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

        flatten = pd.DataFrame(table.stack().values, columns=['value'])
        flatten['value'] = flatten['value'].astype(str)
        flatten['is_index'] = readonly_flag
        flatten['is_readonly'] = readonly_flag
        flatten['is_filtred'] = True
        flatten['dtype'] = flatten['value'].apply(get_dtype)
        flatten['color'] = np.where(flatten['is_index'], '#f8fafd', 'white')

        return flatten
