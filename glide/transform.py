"""A home for common transform nodes"""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import numpy as np
import pandas as pd
from toolbox import set_missing_key

from glide.core import Node


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
