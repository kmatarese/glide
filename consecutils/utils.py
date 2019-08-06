from collections.abc import MutableMapping
import inspect
import os

import pandas as pd
from pprint import pprint as pp


def st():
    __import__("pdb").Pdb().set_trace(inspect.currentframe().f_back)


def rmfile(fname):
    try:
        os.remove(fname)
    except FileNotFoundError:
        pass


def read_chunks(reader, chunksize, limit=None):
    chunk = []
    for i, line in enumerate(reader):
        if limit and i >= limit:
            break
        if i % chunksize == 0 and i > 0:
            yield chunk
            del chunk[:]
        chunk.append(line)
    yield chunk


def set_missing_key(d, k, v):
    # Sets a key in a dict, only if its missing
    if k not in d:
        d[k] = v


# https://stackoverflow.com/questions/16804234/python-wrap-and-object-into-a-list-if-not-is-an-iterable
def isstr(o):
    try:
        basestring
    except NameError:
        basestring = (str, bytes)
    return isinstance(o, basestring)


def is_pandas(o):
    return isinstance(o, (pd.DataFrame, pd.Series, pd.Panel))


def iterize(o):
    if is_pandas(o):
        return o

    if inspect.isgenerator(o):
        return o

    if not isstr(o):
        try:
            return list(o)
        except TypeError:
            pass

    return [o]


class MappingMixin(MutableMapping):
    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]

    def __delitem__(self, key):
        del self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)
