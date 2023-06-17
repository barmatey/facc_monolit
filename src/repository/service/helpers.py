import pandas as pd


def split_dataframe(df: pd.DataFrame, chunk: int) -> list[pd.DataFrame]:
    if chunk <= 0:
        raise Exception

    result = []

    start = 0
    end = start + chunk

    while start < len(df):
        result.append(df.iloc[start:end])
        start += chunk
        end += chunk

    return result


def split_list(data: list, chunk: int) -> list[list]:
    if chunk <= 0:
        raise Exception

    result = []

    start = 0
    end = start + chunk

    while start < len(data):
        result.append(data[start:end])
        start += chunk
        end += chunk

    return result
