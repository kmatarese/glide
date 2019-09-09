"""Common utilities"""

from collections.abc import MutableMapping
from inspect import isgenerator
import io
import os

import pandas as pd
from pprint import pprint as pp
from toolbox import st, read_chunks, set_missing_key, is_str, MappingMixin


def is_pandas(o):
    return isinstance(o, (pd.DataFrame, pd.Series, pd.Panel))


def is_file_obj(o):
    return isinstance(o, (io.TextIOBase, io.BufferedIOBase, io.RawIOBase, io.IOBase))


def iterize(o):
    """Automatically wrap certain objects that you would not normally process item by item"""
    if (
        is_pandas(o)
        or isgenerator(o)
        or is_str(o)
        or is_file_obj(o)
        or isinstance(o, dict)
        or callable(o)
    ):
        return [o]
    return o
