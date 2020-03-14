import asyncio
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import TimeoutError
import datetime
import shutil
import sys
import time

import pytest
import requests

from .test_utils import *
from glide import *


def test_placeholder_node(rootdir):
    nodes = PlaceholderNode("extract") | CSVLoad("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider["extract"] = CSVExtract("extract")
    with open(outfile, "w") as f:
        glider.consume([infile], extract=dict(chunksize=10, nrows=20), load=dict(f=f))


def test_shell_node(rootdir):
    nodes = CSVExtract("extract") | CSVLoad("load") | Shell("shell") | Print("print")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume(
            [infile],
            extract=dict(chunksize=10, nrows=20),
            load=dict(f=f),
            shell=dict(cmd="ls -lt %s" % outfile, shell=True, capture_output=True),
        )


def test_profiler_node(rootdir):
    nodes = Profile("profile") | CSVExtract("extract") | CSVLoad("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], extract=dict(chunksize=10, nrows=20), load=dict(f=f))


def test_filter_node(rootdir):
    nodes = (
        CSVExtract("extract")
        | Filter("filter", func=lambda n, d: len(d) == 5)
        | Reduce("reduce", flatten=True)
        | LenPrint("len")
        | CSVLoad("load")
        | AssertFunc("length_check", func=lambda n, d: len(d) == 5)
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], extract=dict(chunksize=10, nrows=15), load=dict(f=f))


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


def test_window_push(rootdir):
    nodes = (
        CSVExtract("extract", nrows=5) | WindowPush("window", size=3) | Print("print")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile])


def test_window_reduce(rootdir):
    nodes = (
        CSVExtract("extract", nrows=5)
        | IterPush("iter")
        | WindowReduce("window", size=3)
        | Print("print")
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


def test_dict_key_transform(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | DictKeyTransform(
            "transform", **{"zip code": lambda x: x["Zip_Code"]}, drop=["Zip_Code"]
        )
        | PrettyPrint("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile])


def test_hash_key(rootdir):
    nodes = CSVExtract("extract", nrows=10) | HashKey("transform") | PrettyPrint("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile])


def get_json_helper(url, **kwargs):
    resp = requests.get(url, **kwargs)
    return resp.json()


def test_poll_func(rootdir):
    glider = Glider(
        PollFunc(
            "poll",
            func=get_json_helper,
            result_param="id",
            result_value=1,
            data_param="title",
        )
        | Print("print")
    )
    glider.consume(["https://jsonplaceholder.typicode.com/todos/1"])


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


if sys.version_info.major == 3 and sys.version_info.minor >= 7:

    async def async_sleep(x):
        # Simulate some async functionality, then pass data through
        await asyncio.sleep(0.5)
        return x

    def test_asyncio_submit_push_results(rootdir):
        nodes = (
            CSVExtract("extract", nrows=5)
            | AsyncIOSubmit("transform", push_type=PushTypes.Result)
            | Print("load")
        )
        glider, infile, outfile = file_glider(rootdir, "csv", nodes)
        glider.consume([infile], transform=dict(func=async_sleep))

    def test_asyncio_submit_manage_loop(rootdir):
        loop = asyncio.get_event_loop()
        try:
            nodes = (
                CSVExtract("extract", nrows=5)
                | AsyncIOSubmit("transform", push_type=PushTypes.Result)
                | Print("load")
            )
            glider, infile, outfile = file_glider(rootdir, "csv", nodes)
            glider.consume([infile], transform=dict(func=async_sleep))
        finally:
            loop.close()

    def test_asyncio_submit_push_futures(rootdir):
        nodes = (
            CSVExtract("extract", nrows=5)
            | AsyncIOSubmit("transform", push_type=PushTypes.Async)
            | AsyncIOFuturesReduce("reduce", flatten=True, close=True)
            | Print("load")
        )
        glider, infile, outfile = file_glider(rootdir, "csv", nodes)
        start = time.time()
        glider.consume([infile], transform=dict(func=async_sleep))
        print("Took %.2fs" % (time.time() - start))

    def test_asyncio_timeout(rootdir):
        nodes = (
            CSVExtract("extract", nrows=5)
            | AsyncIOSubmit("transform", push_type=PushTypes.Result, timeout=0.1)
            | Print("load")
        )
        glider, infile, outfile = file_glider(rootdir, "csv", nodes)
        with pytest.raises(asyncio.TimeoutError):
            glider.consume([infile], transform=dict(func=async_sleep))


def sleep1(x):
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
        glider.consume([infile], transform=dict(func=sleep1))


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


def test_update_downstream_context(rootdir):
    nodes = CSVExtract("extract", nrows=10) | [
        FormatPrint("print1"),
        FormatPrint("print2"),
    ]
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider["extract"].update_downstream_context(dict(indent=2))
    glider.consume([infile])


class UpdateRequiredContextTest(Node):
    def run(self, data, outfile=None):
        with open(outfile, "w") as f:
            self.update_downstream_context(dict(f=f))
            self.push(data)


def test_update_downstream_context_required_arg(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10) | PlaceholderNode("context") | CSVLoad("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider["context"] = UpdateRequiredContextTest("context", outfile=outfile)
    glider.consume([infile])


def test_context_push_node(rootdir):
    nodes = (
        CSVExtract("extract", nrows=10)
        | ContextPush("context", func=lambda node, data: dict(indent=4))
        | [FormatPrint("print1"), FormatPrint("print2")]
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile])


def test_config_context_json(rootdir):
    nodes = CSVExtract(
        "extract", nrows=ConfigContext("config_context.json", key="nrows")
    ) | LenPrint("print")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile])


def test_config_context_yaml(rootdir):
    nodes = CSVExtract(
        "extract", nrows=ConfigContext("config_context.yaml", key="nrows")
    ) | LenPrint("print")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile])


def test_config_context_ini(rootdir):
    nodes = CSVExtract(
        "extract",
        nrows=ConfigContext(
            "config_context.ini", key=lambda x: int(x["TEST"]["nrows"])
        ),
    ) | LenPrint("print")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile])


def test_datetime_window_push():
    nodes = DateTimeWindowPush("windows") | PrettyPrint("print")
    glider = Glider(nodes)
    today = datetime.date.today()
    glider.consume(
        None,
        windows=dict(
            start_date=today - datetime.timedelta(days=3), end_date=today, num_windows=2
        ),
    )


def test_date_window_push():
    nodes = DateWindowPush("windows") | PrettyPrint("print")
    glider = Glider(nodes)
    today = datetime.date.today()
    now = datetime.datetime.now()
    glider.consume(
        None,
        windows=dict(
            start_date=datetime.datetime(2019, 10, 25, 3, 2, 1),
            end_date=datetime.datetime(2019, 10, 28, 3, 2, 1),
        ),
    )
