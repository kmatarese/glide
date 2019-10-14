from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import TimeoutError
import time

import pytest

from .test_utils import *
from glide import *


def test_placeholder_node(rootdir):
    nodes = PlaceholderNode("extract") | CSVLoad("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider["extract"] = CSVExtract("extract")
    glider.consume(
        [infile], extract=dict(chunksize=100, nrows=20), load=dict(f=outfile)
    )


def test_process_pool_submit(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | ProcessPoolSubmit("transform", push_type=PushTypes.Result)
        | Print("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], transform=dict(func=lower_rows))


def test_process_pool_reducer(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | ProcessPoolSubmit("transform")
        | FuturesReduce("reducer", flatten=True)
        | Print("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], transform=dict(func=lower_rows))


def test_thread_pool_submit(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | ThreadPoolSubmit(
            "transform", push_type=PushTypes.Result, executor_kwargs=dict(max_workers=4)
        )
        | Print("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], transform=dict(func=lower_rows))


def test_pool_submit_executor_param(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | ThreadPoolSubmit(
            "transform", push_type=PushTypes.Result, executor_kwargs=dict(max_workers=4)
        )
        | Print("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with ThreadPoolExecutor(max_workers=4) as executor, open(outfile, "w") as f:
        glider.consume([infile], transform=dict(func=lower_rows, executor=executor))


def sleep2(x):
    time.sleep(1)


def test_pool_timeout(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | ProcessPoolSubmit(
            "transform",
            push_type=PushTypes.Result,
            executor_kwargs=dict(max_workers=4),
            timeout=0.5,
        )
        | Print("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with pytest.raises(TimeoutError), open(outfile, "w") as f:
        glider.consume([infile], transform=dict(func=sleep2))


def test_flatten(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | ProcessPoolSubmit("transform")
        | FuturesReduce("reducer", flatten=False)
        | Flatten("flatten")
        | Print("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], transform=dict(func=lower_rows))
