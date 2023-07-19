import typing

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
        logger.success(f'function {func.__name__} took {total_time*1_000:.0f}ms')
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
