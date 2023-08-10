import typing

import loguru
import pandas as pd
from loguru import logger
from functools import wraps
import time


def async_timeit(func):
    @wraps(func)
    async def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = await func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.success(f'function {func.__name__} took {total_time * 1_000:.0f}ms')
        return result

    return timeit_wrapper


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.success(f'function {func.__name__} took {total_time * 1000:.0f}ms')
        return result

    return timeit_wrapper


def array_split(array: typing.Iterable, count_cols: int):
    start = 0
    end = len(array)
    return [array[x:x + count_cols] for x in range(start, end, count_cols)]


def mixed_frame_sort(df: pd.DataFrame, sort_by: list | str) -> pd.DataFrame:
    df = df.copy()
    if isinstance(sort_by, str):
        sort_by = [sort_by]
    sortcols = []
    sort_pairs = []
    for col in sort_by:
        sortcol = f'__{col}__'
        sortcols.append(sortcol)
        sort_pairs.append(sortcol)
        sort_pairs.append(col)
        df[sortcol] = pd.to_numeric(df[col], errors='coerce')
    df.sort_values(sort_pairs, inplace=True, ignore_index=True)
    df.drop(sortcols, axis=1, inplace=True)
    return df


def log(*args):
    s = "\n\n"
    for arg in args:
        s += str(arg)
    s += "\n\n"
    loguru.logger.debug(s)