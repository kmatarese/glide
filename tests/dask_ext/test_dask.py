from glide.extensions.dask import *
from ..test_utils import *


# -------- Transformers


def test_csv_dask_client_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DataFrameDaskClientTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile], transform=dict(func=df_lower), load=dict(f=outfile))


def test_csv_dask_client_threads_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DataFrameDaskClientTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume(
        [infile],
        transform=dict(func=df_lower, executor_kwargs=dict(processes=False)),
        load=dict(f=outfile),
    )


def test_csv_dask_dataframe_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DaskDataFrameApplyTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile], transform=dict(func=lower), load=dict(f=outfile))


# -------- Parallel Nodes


def test_dask_delayed_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(
        RowCSVExtractor("extract", nrows=10)
        | DaskDelayedPush("push")
        | [Printer("load1"), Printer("load2"), Printer("load3")]
    )
    glider.consume([infile])


def test_dask_client_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(
        RowCSVExtractor("extract", nrows=10)
        | DaskClientPush("push")
        | [Printer("load1"), Printer("load2"), Printer("load3")]
    )
    glider.consume([infile])


# -------- ParaGlider


def test_csv_dask_paraglider(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = DaskParaGlider(RowCSVExtractor("extract") | Printer("load"))
    glider.consume([infile], extract=dict(nrows=10))
