from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import TimeoutError
import shutil
import time

import pytest

from .test_utils import *
from glide import *


def test_placeholder_node(rootdir):
    nodes = PlaceholderNode("extract") | CSVLoad("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider["extract"] = CSVExtract("extract")
    with open(outfile, "w") as f:
        glider.consume([infile], extract=dict(chunksize=10, nrows=20), load=dict(f=f))


def test_assert_node(rootdir):
    nodes = (
        CSVExtract("extract", chunksize=10, nrows=20)
        | AssertFunc("length_check", func=lambda node, data: len(data) == 10)
        | CSVLoad("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], load=dict(f=f))


def parity_zip_router(row):
    if int(row["Zip_Code"]) % 2 == 0:
        return "even"
    return "odd"


def threshold_zip_router(row):
    zipcode = int(row["Zip_Code"])
    prepend = "odd"
    if zipcode % 2 == 0:
        prepend = "even"
    if zipcode >= 1020:
        return "%s_large" % prepend
    return "%s_small" % prepend


def test_router_function(rootdir):
    nodes = (
        CSVExtract("extract", nrows=20)
        | IterPush("iter")
        | [parity_zip_router, Print("even"), Print("odd")]
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile])


class ZipGroupByDMA(GroupByNode):
    def key(self, row):
        key = row["DMA_Description"]
        return key

    def run(self, batch):
        self.push({batch[0]["DMA_Description"]: [z["Zip_Code"] for z in batch]})


def test_group_by_node(rootdir):
    nodes = (
        CSVExtract("extract", nrows=20)
        | IterPush("iter")
        | ZipGroupByDMA("group")
        | Print("print")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile])


def test_complex_pipeline(rootdir):
    nodes = (
        CSVExtract("extract", nrows=40)
        | IterPush("iter")
        | [
            parity_zip_router,
            (
                Print("even")
                | [threshold_zip_router, Print("even_large"), Print("even_small")]
            ),
            (
                Print("odd")
                | [threshold_zip_router, Print("odd_large"), Print("odd_small")]
            ),
        ]
    )

    glider, infile, outfile = file_glider(rootdir, "csv", nodes)

    large = Print("large")
    small = Print("small")
    reducer = Reduce("reduce")
    combined = LenPrint("combined")

    large.add_downstream(reducer)
    small.add_downstream(reducer)
    reducer.add_downstream(combined)

    glider["even_large"].add_downstream(large)
    glider["odd_large"].add_downstream(large)
    glider["even_small"].add_downstream(small)
    glider["odd_small"].add_downstream(small)

    glider.consume([infile])
    if shutil.which("dot"):
        filename = "%s/pipeline_plot.png" % test_config["OutputDirectory"]
        print("Found dot package, printing pipeline graph to %s" % filename)
        glider.plot(filename)


def test_map(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | Map("transform", func=row_lower, as_list=True)
        | CSVLoad("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], load=dict(f=f))


def test_func(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | Func("transform", func=lower_rows)
        | Print("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile])


def test_process_pool_submit(rootdir):
    nodes = (
        CSVExtract("extract", nrows=100)
        | ProcessPoolSubmit("transform", push_type=PushTypes.Result)
        | CSVLoad("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], transform=dict(func=lower_rows), load=dict(f=f))


def test_process_pool_reducer(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | ProcessPoolSubmit("transform")
        | FuturesReduce("reducer", flatten=True)
        | Print("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
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
    glider.consume([infile], transform=dict(func=lower_rows))
