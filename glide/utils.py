"""Common utilities"""

from inspect import isgenerator
import io

import pandas as pd
from toolbox import st, is_str


def is_pandas(o):
    return isinstance(o, (pd.DataFrame, pd.Series, pd.Panel))


def is_file_obj(o):
    return isinstance(o, (io.TextIOBase, io.BufferedIOBase, io.RawIOBase, io.IOBase))


def iterize(o):
    """Automatically wrap certain objects that you would not normally process item by item"""
    if (
        is_pandas(o)
        or is_str(o)
        or is_file_obj(o)
        or isinstance(o, dict)
        or callable(o)
    ):
        return [o]
    return o
