import datetime

import loguru
import numpy as np
import pandas as pd

from src import helpers
from src.sheet import enums


class Normalizer:

    def __init__(self, df: pd.DataFrame, drop_index: bool, drop_columns: bool, readonly_all_cells: bool = False):
        self.df = df.copy()
        self.drop_index = drop_index
        self.drop_columns = drop_columns
        self.readonly_all_cells = readonly_all_cells

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

        rows: pd.DataFrame = self.create_sindex_df(len(table.index), vertical_difference, item_size=30)
        cols: pd.DataFrame = self.create_sindex_df(len(table.columns), horizontal_difference, item_size=120)
        cells: pd.DataFrame = self.create_cell_df(rows, cols, table)

        if self.readonly_all_cells:
            cells['is_readonly'] = True
            rows['is_readonly'] = True
            cols['is_readonly'] = True

        self.rows = rows
        self.cols = cols
        self.cells = cells

    @staticmethod
    def create_sindex_df(total_item_count: int, freeze_item_count: int, item_size: int) -> pd.DataFrame:
        sindex = pd.DataFrame(
            [],
            index=range(0, total_item_count),
            columns=['size', 'is_freeze', 'is_filtred', 'is_readonly', 'index', 'scroll_pos']
        )
        sindex['size'] = item_size
        sindex['is_freeze'] = False
        sindex['is_filtred'] = True
        sindex['is_readonly'] = False
        sindex['index'] = sindex.index
        sindex['scroll_pos'] = sindex['size'].cumsum() - item_size * freeze_item_count
        sindex['scroll_pos'] = np.where(sindex['scroll_pos'] < 0, -1, sindex['scroll_pos'])

        if freeze_item_count > 0:
            sindex.iloc[0:freeze_item_count, 1] = True
            sindex.iloc[0:freeze_item_count, 3] = True

        return sindex

    @staticmethod
    def create_cell_df(rows: pd.DataFrame, cols: pd.DataFrame, table: pd.DataFrame) -> pd.DataFrame:
        col_is_freeze = cols['is_freeze'].tolist() * len(rows.index)
        row_is_freeze = np.repeat(rows['is_freeze'].tolist(), len(cols.index))
        index_flag = np.where(
            row_is_freeze, True, False
        )
        readonly_flag = np.where(
            np.logical_or(row_is_freeze, col_is_freeze), True, False
        )

        def get_dtype(value):
            value_type = type(value)
            if value_type is int or value_type is float:
                return enums.CellDtype.NUMBER.value
            if value_type is bool:
                return enums.CellDtype.BOOLEAN.value
            if isinstance(value, (datetime.date,  datetime.datetime)):
                return enums.CellDtype.DATE.value
            return enums.CellDtype.TEXT.value

        flatten = pd.DataFrame(table.stack().values, columns=['value'])
        flatten['is_index'] = index_flag
        flatten['is_readonly'] = readonly_flag
        flatten['is_filtred'] = True
        flatten['dtype'] = flatten['value'].apply(get_dtype)
        flatten['value'] = flatten['value'].astype(str)
        flatten['value'] = np.where(
            np.logical_and(col_is_freeze, row_is_freeze), '', flatten['value']
        )
        flatten['color'] = np.where(flatten['is_readonly'], '#f8fafd', 'white')

        flatten['text_align'] = np.where(
            np.logical_or(
                flatten['dtype'] == enums.CellDtype.NUMBER.value, flatten['dtype'] == enums.CellDtype.DATE.value),
            'right', 'left'
        )

        return flatten


class Denormalizer:
    def __init__(self, rows: pd.DataFrame, cols: pd.DataFrame, cells: pd.DataFrame):
        self.rows = rows.copy()
        self.cols = cols.copy()
        self.cells = cells.copy()
        self.df = pd.DataFrame([])

    def get_denormalized(self) -> pd.DataFrame:
        return self.df

    def denormalize(self):
        top_index = self.rows['is_freeze'].sum()
        left_index = self.cols['is_freeze'].sum()
        self.df = self.cells_to_table(self.cells, count_cols=len(self.cols), top_index=top_index, left_index=left_index)

    def cells_to_table(self, cells: pd.DataFrame, count_cols: int, top_index: int, left_index: int) -> pd.DataFrame:
        text_type = enums.CellDtype.TEXT.value
        number_type = enums.CellDtype.NUMBER.value
        bool_type = enums.CellDtype.BOOLEAN.value

        value = cells['value'].copy()
        value.loc[cells['dtype'] == text_type] = value.loc[cells['dtype'] == text_type].astype(str)
        value.loc[cells['dtype'] == number_type] = value.loc[cells['dtype'] == number_type].astype(float)
        value.loc[cells['dtype'] == bool_type] = self._convert_to_boolean(value.loc[cells['dtype'] == bool_type])

        values_list = value.tolist()
        table = helpers.array_split(values_list, count_cols)
        table = pd.DataFrame(table)

        if top_index > 0:
            table.columns = self._create_index(table.head(top_index))
            table = table[:][top_index:].reset_index(drop=True)

        # todo I am not sure that this code is correct
        if left_index > 0:
            table_cols = list(table.columns[left_index:])
            index_cols = [f"__lvl_{i}" for i in range(0, left_index)]
            table.columns = index_cols + table_cols
            table = table.set_index(index_cols)

        if 'reverse' in table.columns:
            table['reverse'] = table['reverse'].astype(bool)

        return table

    @staticmethod
    def _convert_to_boolean(series: pd.Series) -> pd.Series:
        series: pd.Series = series.str.lower()
        if len(set(series.unique()) - {'false', 'true'}):
            raise ValueError(f"unique values are not true or false only; {sorted(series.unique().tolist())}")
        converted = series.map({"true": True, "false": False})
        return converted

    @staticmethod
    def _create_index(data: pd.DataFrame) -> pd.Index:
        if len(data) == 1:
            index = pd.Index(data.iloc[0], name='index')
            return index
        elif len(data) > 1:
            names = [f"lvl_{i}" for i in range(0, len(data))]
            index = pd.MultiIndex.from_frame(data.transpose(), names=names)
            return index
        raise Exception
