from typing import Callable

from pandas import concat, DataFrame, Series


def flatmap(df: DataFrame, func: Callable[[Series, ...], DataFrame], *args, **kwargs):
    dfs = []
    for idx, row in df.iterrows():
        df = func(row, *args, **kwargs)
        dfs.append(df)
    return concat(dfs)
