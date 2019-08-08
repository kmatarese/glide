"""A home for common transform nodes"""

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
    """Passthrough transformer"""
    def run(self, item):
        """Just pushes the item through"""
        self.push(item)


# -------- Pandas Transformers


class DataFrameApplyMapTransformer(Node):
    """Apply a transform to a Pandas DataFrame"""
    def run(self, df, func, **kwargs):
        """Use applymap() on a DataFrame"""
        df = df.applymap(func, **kwargs)
        self.push(df)


class SwifterApplyTransformer(Node):
    """Apply a Swifter transform to a Pandas DataFrame"""
    def run(self, df, func, processes=True, **kwargs):
        """Use Swifter apply() on a DataFrame"""
        assert swifter, "The swifter package is not installed"
        if processes:
            df.swifter.set_dask_scheduler(scheduler="processes")
        else:
            df.swifter.set_dask_scheduler(scheduler="threads")
        for column in df.columns:
            df[column] = df[column].swifter.apply(func, **kwargs)
        self.push(df)


class DataFrameProcessPoolTransformer(Node):
    """Apply a transform to a Pandas DataFrame using parallel processes"""
    def run(self, df, func, **kwargs):
        """Split the DataFrame and call func() in parallel processes, concat results"""
        with ProcessPoolExecutor() as executor:
            df_split = np.array_split(df, executor._max_workers)
            df = pd.concat(executor.map(func, df_split, **kwargs))
        self.push(df)


class DataFrameThreadPoolTransformer(Node):
    """Apply a transform to a Pandas DataFrame using parallel threads"""
    def run(self, df, func, **kwargs):
        """Split the DataFrame and call func() in parallel threads, concat results"""
        with ThreadPoolExecutor() as executor:
            df_split = np.array_split(df, executor._max_workers)
            df = pd.concat(executor.map(func, df_split, **kwargs))
        self.push(df)


class DataFrameDaskClientTransformer(Node):
    """Apply a transform to a Pandas DataFrame using dask Client"""
    def run(self, df, func, executor_kwargs=None, **kwargs):
        """Split the DataFrame and call func() using dask Client, concat results"""
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
    """Apply a transform to a Pandas DataFrame using dask dataframe"""

    # NOTE: it may be more efficient to not convert to/from Dask Dataframe in
    # this manner depending on the pipeline
    def run(self, df, func, from_pandas_kwargs=None, **kwargs):
        """Convert to dask dataframe and use apply()"""
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
    """Apply a transform function to a set of rows in parallel processes"""

    def run(self, rows, func, **kwargs):
        """Use a ProcessPoolExecutor to map() func over input rows"""
        result_rows = []
        with ProcessPoolExecutor() as executor:
            set_missing_key(
                kwargs, "chunksize", int(max(len(rows) / executor._max_workers, 1))
            )
            rows = [x for x in executor.map(func, rows, **kwargs)]
        self.push(rows)


class RowThreadPoolTransformer(Node):
    """Apply a transform function to a set of rows in parallel threads"""

    def run(self, rows, func, **kwargs):
        """Use a ThreadPoolExecutor to map() func over input rows"""
        result_rows = []
        with ThreadPoolExecutor() as executor:
            rows = [x for x in executor.map(func, rows, **kwargs)]
        self.push(rows)
