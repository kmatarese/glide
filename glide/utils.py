"""Common utilities"""

import argparse
import asyncio
from collections import defaultdict
import configparser
from contextlib import contextmanager
import datetime
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta
import functools
import itertools
import io
import logging
import math
import types

import numpy as np
import pandas as pd
from pandas.io.common import get_filepath_or_buffer

try:
    from pandas.io.common import _get_handle as get_handle
except ImportError:
    from pandas.io.common import get_handle
from pyexcel_xlsx import get_data as get_xlsx, save_data as save_xlsx
from pyexcel_xls import get_data as get_xls, save_data as save_xls
from tlbx import (
    st,
    raiseif,
    raiseifnot,
    json,
    dbg as _dbg,
    dbgsql as _dbgsql,
    info as _info,
    warn as _warn,
    error as _error,
    get_caller,
    is_str,
    Arg,
    Parent,
)
import yaml

default_logger = logging.getLogger("glide")
default_logger.setLevel(logging.INFO)

XLS = "xls"
XLSX = "xlsx"

_CLASS_LIST_DOCSTRING_TEMPLATE = """

**{heading}:**

{classes_str}
"""


def find_class_in_dict(cls, d, include=None, exclude=None):
    """Get a list of keys that are an instance of a class in a dict"""
    names = []
    for key, value in d.copy().items():
        if not isinstance(value, type):
            continue
        if issubclass(value, cls):
            if include and include not in key:
                continue
            if exclude and exclude in key:
                continue
            names.append(key)
    return names


def get_class_list_docstring(heading, classes):
    """Helper to generate a part of a module docstring from a list of classes"""
    classes_str = "- " + "\n- ".join(classes)
    return _CLASS_LIST_DOCSTRING_TEMPLATE.format(
        heading=heading, classes_str=classes_str
    )


def closer(x):
    """Helper to call close on x"""
    x.close()


def not_none(*args):
    return all([x is not None for x in args])


def split_count_helper(data, split_count):
    """Helper to override the split count if data len is shorter"""
    if hasattr(data, "__len__"):
        return min(len(data), split_count)
    return split_count


# -------- Date utils


def date_from_str(s):
    today = datetime.date.today()
    if s == "today":
        return today
    elif s == "yesterday":
        return today - datetime.timedelta(days=1)
    elif s == "tomorrow":
        return today + datetime.timedelta(days=1)
    elif s == "start_of_month":
        return today.replace(day=1)
    elif s == "start_of_next_month":
        return (today + relativedelta(months=+1)).replace(day=1)
    elif s == "start_of_last_month":
        return (today.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)

    return dateparser.parse(s)


def to_datetime(d):
    if type(d) == datetime.datetime:
        return d
    if type(d) == datetime.date:
        return datetime.datetime.combine(d, datetime.time()).replace(microsecond=0)
    if type(d) == str:
        return to_datetime(date_from_str(d))
    raise AssertionError("Unsupported type: %s" % type(d))


def to_date(d):
    if type(d) == datetime.date:
        return d
    if type(d) == datetime.datetime:
        return d.date()
    if type(d) == str:
        return to_date(date_from_str(d))
    raise AssertionError("Unsupported type: %s" % type(d))


def datetime_cmp(d1, d2):
    d1 = to_datetime(d1)
    d2 = to_datetime(d2)
    return (d1 > d2) - (d1 < d2)


def get_date_windows(start_date, end_date, reverse=False):
    start_date = to_datetime(start_date)
    end_date = to_datetime(end_date)

    start_date = start_date.date()
    if datetime_cmp(end_date, end_date.date()) != 0:
        end_date = end_date.date() + datetime.timedelta(days=1)
    else:
        end_date = end_date.date()

    delta = end_date - start_date
    num_windows = delta.days
    orig_end_date = end_date
    end_date = start_date
    date_windows = []

    for i in range(num_windows):
        end_date = end_date + datetime.timedelta(days=1)
        date_windows.append((start_date, end_date))
        start_date = end_date
        if end_date >= orig_end_date:
            break

    if reverse:
        date_windows.reverse()

    return date_windows


def get_datetime_windows(
    start_date,
    end_date,
    window_size_hours=None,
    num_windows=None,
    reverse=False,
    add_second=True,
):
    """Produce a list of start/end date tuples

    Parameters
    ----------
    start_date : date, datetime, or str
        The absolute start date of the range
    end_date : date, datetime, or str
        The absolute end date of the range
    window_size_hours : float, optional
        The size of the windows in hours. May be a float to represent partial hours.
    num_windows : int, optional
        The number of windows to split the date range into. One of num_windows or
        window_size_hours must be specified.
    reverse : bool, optional
        If true return the windows in reverse order
    add_second : bool, optional
        If true, offset the start of each window to be one second past the end
        date of the previous window.

    Returns
    -------
    dt_windows : list
        A list of tuples of start / end datetime pairs

    """
    raiseifnot(
        window_size_hours or num_windows,
        "One of window_size_hours or num_windows must be specified",
    )
    raiseif(
        window_size_hours and num_windows,
        "Only one of window_size_hours or num_windows can be specified",
    )

    start_date = to_datetime(start_date)
    end_date = to_datetime(end_date)
    total_secs = (end_date - start_date).total_seconds()

    if num_windows:
        if num_windows == 1:
            return [(start_date, end_date)]
        window_size_hours = (total_secs / num_windows) / 3600

    total_hours = total_secs / 3600
    num_windows = num_windows or int(math.ceil(total_hours / window_size_hours))
    window_delta = datetime.timedelta(hours=window_size_hours)
    orig_end_date = end_date
    end_date = start_date
    dt_windows = []

    for i in range(num_windows):
        end_date = min(end_date + window_delta, orig_end_date)
        dt_windows.append((start_date, end_date))
        seconds = 1 if add_second else 0
        start_date = end_date + datetime.timedelta(seconds=seconds)
        if end_date >= orig_end_date:
            break

    if reverse:
        dt_windows.reverse()

    return dt_windows


def _process_date_action_dest(dest, values, end_date):
    today = datetime.date.today()
    now = datetime.datetime.now()
    if dest == "yesterday":
        start_date = today - datetime.timedelta(days=1)
        end_date = today
    elif dest == "today":
        start_date = today
    elif dest == "days_back":
        start_date = today - datetime.timedelta(days=values)
    elif dest == "hours_back":
        start_date = now - datetime.timedelta(hours=values)
    elif dest == "date_range":
        if len(values) == 1:
            start_date = date_from_str(values[0])
        elif len(values) == 2:
            start_date = date_from_str(values[0])
            end_date = date_from_str(values[1])
        else:
            raise AssertionError("date_range only accepts one or two arguments")
    else:
        raise AssertionError("Unrecognized date action %s" % dest)
    return start_date, end_date


class DateTimeWindowAction(argparse.Action):
    """An argparse Action for handling datetime window CLI args"""

    def __call__(self, parser, namespace, values, option_string=None):
        end_date = datetime.datetime.now()

        if self.dest == "date_window_size":
            if getattr(namespace, "date_windows", None):
                start_date = namespace.date_windows[0][0]
                end_date = namespace.date_windows[0][1]
                setattr(
                    namespace,
                    "date_windows",
                    get_datetime_windows(
                        start_date,
                        end_date,
                        window_size_hours=values,
                        reverse=getattr(namespace, "reverse_windows", False),
                    ),
                )
            else:
                # This will get used later once date_windows is populated
                setattr(namespace, self.dest, values)
                return
        else:
            start_date, end_date = _process_date_action_dest(
                self.dest, values, end_date
            )

        if not hasattr(namespace, "date_windows"):
            if getattr(namespace, "date_window_size", None):
                setattr(
                    namespace,
                    "date_windows",
                    get_datetime_windows(
                        start_date,
                        end_date,
                        window_size_hours=namespace.date_window_size,
                        reverse=getattr(namespace, "reverse_windows", False),
                    ),
                )
            else:
                setattr(namespace, "date_windows", [(start_date, end_date)])


class DateWindowAction(argparse.Action):
    """An argparse Action for handling date window CLI args"""

    def __call__(self, parser, namespace, values, option_string=None):
        today = datetime.date.today()
        end_date = today + datetime.timedelta(days=1)
        start_date, end_date = _process_date_action_dest(self.dest, values, end_date)

        raiseifnot(
            isinstance(start_date, datetime.date),
            "start date must be a datetime.date object",
        )
        raiseifnot(
            isinstance(end_date, datetime.date),
            "end date must be a datetime.date object",
        )

        date_windows = get_date_windows(
            start_date, end_date, reverse=getattr(namespace, "reverse_windows", False)
        )
        setattr(namespace, "date_windows", date_windows)


@Parent()
@Arg("--today", nargs=0, action=DateTimeWindowAction, help="Use today as date range")
@Arg(
    "--yesterday",
    nargs=0,
    action=DateTimeWindowAction,
    help="Use yesterday as date range",
)
@Arg(
    "--days-back",
    type=int,
    action=DateTimeWindowAction,
    help="Use date range starting N days back",
)
@Arg(
    "--hours-back",
    type=int,
    action=DateTimeWindowAction,
    help="Use date range starting N hours back",
)
@Arg(
    "--date-range",
    nargs="+",
    metavar=("start_date", "end_date"),
    type=str,
    action=DateTimeWindowAction,
    help="Specify custom date range start/end",
)
@Arg(
    "--date-window-size",
    type=float,
    action=DateTimeWindowAction,
    help="Date window size in hours",
)
def datetime_window_cli():
    """An argparse parent CLI that adds datetime window support"""
    pass


@Parent()
@Arg("--today", nargs=0, action=DateWindowAction, help="Use today as date range")
@Arg(
    "--yesterday", nargs=0, action=DateWindowAction, help="Use yesterday as date range"
)
@Arg(
    "--days-back",
    type=int,
    action=DateWindowAction,
    help="Use date range starting N days back",
)
@Arg(
    "--date-range",
    nargs="+",
    metavar=("start_date", "end_date"),
    type=str,
    action=DateWindowAction,
    help="Specify custom date range start/end",
)
def date_window_cli():
    """An argparse parent CLI that adds date window support"""
    pass


# -------- Config utils


def _config_helper(data, key):
    if key:
        if callable(key):
            return key(data)
        return data[key]
    return data


def load_json_config(filename, key=None):
    """Load a config from a json file, optionally extracting a key"""
    with open(filename, "r") as f:
        config = json.load(f)
    return _config_helper(config, key)


def load_yaml_config(filename, key=None):
    """Load a config from a yaml file, optionally extracting a key"""
    with open(filename, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    return _config_helper(config, key)


def load_ini_config(filename, key=None):
    """Load a config from an ini file, optionally extracting a key"""
    config = configparser.ConfigParser()
    config.read(filename)
    return _config_helper(config, key)


# -------- Type utils


def is_function(f):
    """Test if f is a function"""
    return isinstance(f, (types.FunctionType, functools.partial))


def is_pandas(o):
    """Test if an object is a Pandas object"""
    return isinstance(o, (pd.DataFrame, pd.Series))


def is_file_obj(o):
    """Test if an object is a file object"""
    return isinstance(o, (io.TextIOBase, io.BufferedIOBase, io.RawIOBase, io.IOBase))


# -------- Iterable utils


def nchunks(a, n):
    """Divide iterable a into n chunks"""
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n))


def divide_data(data, n):
    """Divide data into n chunks, with special handling for pandas objects"""
    if is_pandas(data):
        return np.array_split(data, n)
    else:
        return nchunks(data, n)


def flatten(l):
    """Flatten a list of iterables"""
    raiseifnot(
        l and len(l) > 0, "flatten requires a list of lists or list of pandas objects"
    )
    if is_pandas(l[0]):
        return pd.concat(l)
    else:
        return list(itertools.chain.from_iterable(l))


def size(o, default=None):
    """Helper to return the len() of an object if it is available"""
    if hasattr(o, "__len__"):
        return len(o)
    return default


# https://stackoverflow.com/questions/6822725/rolling-or-sliding-window-iterator
def window(seq, size=2):
    """Returns a sliding window over data from the iterable"""
    it = iter(seq)
    result = tuple(itertools.islice(it, size))
    if len(result) == size:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result


def iterize(o):
    """Automatically wrap certain objects that you would not normally process
    item by item.

    TODO: this function should probaly be improved/generalized.
    """
    if (
        is_pandas(o)
        or is_str(o)
        or is_file_obj(o)
        or isinstance(o, dict)
        or callable(o)
    ):
        return [o]
    return o


def listify(o):
    """Ensure an object is a list by wrapping if necessary"""
    if isinstance(o, list):
        return o
    return [o]


def join(tables, on=None, how="left", rsuffixes=None):
    """Join a list of iterables or DataFrames"""

    if rsuffixes:
        raiseifnot(
            len(rsuffixes) == (len(tables) - 1), "Must have one rsuffix per right table"
        )

    convert_df = True
    if isinstance(tables[0], pd.DataFrame):
        raiseifnot(
            all([isinstance(t, pd.DataFrame) for t in tables]),
            "Expected all items to be DataFrames",
        )
        convert_df = False

    df = None
    for i, table in enumerate(tables):
        if convert_df:
            table = pd.DataFrame.from_records(table, index=on, coerce_float=True)
            # https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html
            # Force new dtypes to avoid int->float coersion when null values are present
            table = table.convert_dtypes()

        if i == 0:
            df = table
            continue

        rsuffix = ""
        if rsuffixes:
            rsuffix = rsuffixes[i - 1]

        df = df.join(table, on=on, how=how, rsuffix=rsuffix)

    if not convert_df:
        return df

    if how != "outer":
        df = df.reset_index()
    df.replace({np.nan: None}, inplace=True)
    rows = df.to_dict(orient="records")
    return rows


# -------- File utils


def open_filepath_or_buffer(f, open_flags="r", compression=None, is_text=True):
    """Use pandas IO functions to return a handle from a filepath
    or buffer.

    Parameters
    ----------
    f : str or buffer
        filepath or buffer to open
    open_flags : str, optional
        mode to open file
    compression : str, optional
        compression arg passed to pandas functions
    is_text : bool
        Whether file/buffer is in text format, Passed through to pandas helpers.

    Returns
    -------
    f : file-like
        A file-like object
    handles : list of file-like
        A list of file-like objects opened. Seems mostly relevant for zipped archives.
    close : bool
        A flag indicating whether the caller should close the file object when done

    """
    f, _, compression, should_close = get_filepath_or_buffer(f, compression=compression)

    close = False or should_close
    if isinstance(f, str):
        close = True

    f, handles = get_handle(f, open_flags, compression=compression, is_text=is_text)

    # TODO: review behavior when handles has multiple files
    return f, handles, close


@contextmanager
def get_file_handle(*args, **kwargs):
    """Context manager pass through to open_filepath_or_buffer. This will
    automatically close the file if and only if it was opened here. If file
    handles are passed in it is assumed the caller will manage them."""
    f, _, close = open_filepath_or_buffer(*args, **kwargs)
    try:
        yield f
    finally:
        if close:
            f.close()


# -------- Excel utils


def excel_file_type(f):
    """Best guess at Excel file type from name"""
    if isinstance(f, str):
        if f.endswith(XLS):
            return XLS
        if f.endswith(XLSX):
            return XLSX
        raise AssertionError("Unsupported Excel file: %s" % f)
    else:
        if hasattr(f, "name") and f.name.endswith(XLS):
            return XLS
        # Just assumes it's an .xlsx file
        return XLSX


def read_excel(f, **kwargs):
    """Read data from an Excel file using pyexcel

    Parameters
    ----------
    f : str or buffer
        Excel file to read from
    **kwargs
        Keyword arguments passed to pyexcel

    """
    excel_type = excel_file_type(f)
    if excel_type == XLS:
        data = get_xls(f, **kwargs)
    else:
        data = get_xlsx(f, **kwargs)
    return data


def save_excel(f, data, **kwargs):
    """Write data to an Excel file using pyexcel

    Note
    ----
    If f is a file that ends in .xls, pyexcel_xls will be used, otherwise it
    defaults to pyexcel_xlsx.

    Parameters
    ----------
    f : str or buffer
        Excel file to write to
    data : dict
        Data to write to the file. This is expected to be a dict of
        {sheet_name: sheet_data} format.
    **kwargs
        Keyword arguments passed to pyexcel's save_data

    """
    excel_type = excel_file_type(f)
    if excel_type == XLS:
        save_xls(f, data, **kwargs)
    else:
        save_xlsx(f, data, **kwargs)


# -------- Asyncio utils


def get_or_create_event_loop():
    """Get an existing asyncio Event Loop or create one if necessary"""
    created = False
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            created = True
    return loop, created


def cancel_asyncio_tasks(tasks, loop, cancel_timeout=None):
    """Cancel a set of asyncio tasks

    Parameters
    ----------
    tasks : iterable
        An iterable of asyncio tasks to cancel
    loop
        asyncio Event Loop
    cancel_timeout : int or float, optional
        A timeout to use when waiting for tasks to finish cancelling

    """
    [task.cancel() for task in tasks]
    _, not_done = loop.run_until_complete(asyncio.wait(tasks, timeout=cancel_timeout))
    if not_done:
        warn("%d tasks did not cancel immediately" % len(not_done))


# -------- Logging utils


def dbg(msg, **kwargs):
    """Call tlbx dbg with glide logger"""
    kwargs["logger"] = kwargs.get("logger", default_logger)
    kwargs["label"] = kwargs.get("label", get_caller())
    _dbg(msg, **kwargs)


def dbgsql(msg, **kwargs):
    """Call tlbx dbgsql with glide logger"""
    kwargs["logger"] = kwargs.get("logger", default_logger)
    kwargs["label"] = kwargs.get("label", get_caller())
    _dbgsql(msg, **kwargs)


def info(msg, **kwargs):
    """Call tlbx info with glide logger"""
    kwargs["logger"] = kwargs.get("logger", default_logger)
    kwargs["label"] = kwargs.get("label", get_caller())
    _info(msg, **kwargs)


def warn(msg, **kwargs):
    """Call tlbx warn with glide logger"""
    kwargs["logger"] = kwargs.get("logger", default_logger)
    kwargs["label"] = kwargs.get("label", get_caller())
    _warn(msg, **kwargs)


def error(msg, **kwargs):
    """Call tlbx error with glide logger"""
    kwargs["logger"] = kwargs.get("logger", default_logger)
    kwargs["label"] = kwargs.get("label", get_caller())
    _error(msg, **kwargs)
