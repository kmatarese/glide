"""Common utilities"""

from collections.abc import MutableMapping
import inspect
import os

import pandas as pd
from pprint import pprint as pp
from toolbox import st, read_chunks, set_missing_key, is_str, MappingMixin


def is_pandas(o):
    return isinstance(o, (pd.DataFrame, pd.Series, pd.Panel))


def iterize(o):
    """Wrap an object in a list unless it already is a non-string iteratable"""
    if is_pandas(o):
        return o

    if inspect.isgenerator(o):
        return o

    if not is_str(o):
        try:
            return list(o)
        except TypeError:
            pass

    return [o]
