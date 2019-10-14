from glide.extensions.dask import *
from glide.extensions.pandas import *
from ..test_utils import *


# -------- Transformers


def test_csv_dask_client_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtract("extract")
        | DaskClientMap("transform", push_type=PushTypes.Result)
        | DataFrameCSVLoad("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile], transform=dict(func=df_lower), load=dict(f=outfile))


def test_csv_dask_client_reducer(rootdir):
    nodes = (
        DataFrameCSVExtract("extract")
        | DaskClientMap("transform")
        | DaskFuturesReduce("reduce", flatten=True)
        | DataFrameCSVLoad("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with Client() as executor:
        glider.consume(
            [infile],
            transform=dict(func=df_lower, executor=executor),
            load=dict(f=outfile),
        )


def test_csv_dask_client_threads_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtract("extract")
        | DaskClientMap("transform", push_type=PushTypes.Result)
        | DataFrameCSVLoad("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume(
        [infile],
        transform=dict(func=df_lower, executor_kwargs=dict(processes=False)),
        load=dict(f=outfile),
    )


def test_csv_dask_dataframe_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtract("extract")
        | DaskDataFrameApply("transform")
        | DataFrameCSVLoad("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile], transform=dict(func=lower), load=dict(f=outfile))


# -------- Parallel Nodes


def test_dask_delayed_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(
        CSVExtract("extract", nrows=10)
        | DaskDelayedPush("push")
        | [Print("load1"), Print("load2"), Print("load3")]
    )
    glider.consume([infile])


def test_dask_client_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(
        CSVExtract("extract", nrows=10)
        | DaskClientPush("push")
        | [Print("load1"), Print("load2"), Print("load3")]
    )
    glider.consume([infile])


# -------- ParaGlider


def test_csv_dask_paraglider(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = DaskParaGlider(CSVExtract("extract") | Print("load"))
    glider.consume([infile], synchronous=True, extract=dict(nrows=10))
