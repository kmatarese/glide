from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

try:
    from dask.dataframe import from_pandas
    from dask.distributed import Client, as_completed
except ImportError:
    from_pandas = None
    Client = None
    as_completed = None
import numpy as np
import pandas as pd

try:
    import swifter
except ImportError:
    swifter = None

from consecutils.core import Node
from consecutils.utils import set_missing_key


class DummyTransformer(Node):
    def run(self, item):
        self.push(item)


# -------- Pandas Transformers


class DataFrameApplyMapTransformer(Node):
    def run(self, df, func, **kwargs):
        df = df.applymap(func, **kwargs)
        self.push(df)


class SwifterApplyTransformer(Node):
    def run(self, df, func, processes=True, **kwargs):
        assert swifter, "The swifter package is not installed"
        if processes:
            df.swifter.set_dask_scheduler(scheduler="processes")
        else:
            df.swifter.set_dask_scheduler(scheduler="threads")
        for column in df.columns:
            df[column] = df[column].swifter.apply(func, **kwargs)
        self.push(df)


class DataFrameProcessPoolTransformer(Node):
    def run(self, df, func, **kwargs):
        with ProcessPoolExecutor() as executor:
            df_split = np.array_split(df, executor._max_workers)
            df = pd.concat(executor.map(func, df_split, **kwargs))
        self.push(df)


class DataFrameThreadPoolTransformer(Node):
    def run(self, df, func, **kwargs):
        with ThreadPoolExecutor() as executor:
            df_split = np.array_split(df, executor._max_workers)
            df = pd.concat(executor.map(func, df_split, **kwargs))
        self.push(df)


class DataFrameDaskClientTransformer(Node):
    def run(self, df, func, executor_kwargs=None, **kwargs):
        assert Client, "The dask (Client) package is not installed"
        # https://distributed.dask.org/en/latest/api.html
        dfs = []
        executor_kwargs = executor_kwargs or {}
        with Client(**executor_kwargs) as client:
            df_split = np.array_split(df, len(client.ncores()))
            futures = client.map(func, df_split, **kwargs)
            for future, result in as_completed(futures, with_results=True):
                dfs.append(result)
        df = pd.concat(dfs)
        self.push(df)


class DaskDataFrameApplyTransformer(Node):
    # NOTE: it may be more efficient to not convert to/from Dask Dataframe in
    # this manner depending on the pipeline
    def run(self, df, func, from_pandas_kwargs=None, **kwargs):
        assert from_pandas, "The dask (dataframe) package is not installed"
        from_pandas_kwargs = from_pandas_kwargs or {}
        set_missing_key(from_pandas_kwargs, "chunksize", 500)
        set_missing_key(from_pandas_kwargs, "sort", False)
        ddf = from_pandas(df, **from_pandas_kwargs)
        for column in ddf.columns:
            ddf[column] = ddf[column].apply(
                func, meta=(column, ddf[column].dtype), **kwargs
            )
        df = ddf.compute()
        self.push(df)


# -------- Row-based Transformers


class RowProcessPoolTransformer(Node):
    def run(self, rows, func, **kwargs):
        result_rows = []
        with ProcessPoolExecutor() as executor:
            set_missing_key(
                kwargs, "chunksize", int(max(len(rows) / executor._max_workers, 1))
            )
            rows = [x for x in executor.map(func, rows, **kwargs)]
        self.push(rows)


class RowThreadPoolTransformer(Node):
    def run(self, rows, func, **kwargs):
        result_rows = []
        with ThreadPoolExecutor() as executor:
            rows = [x for x in executor.map(func, rows, **kwargs)]
        self.push(rows)
