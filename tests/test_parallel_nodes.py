from .test_utils import *
from glide import *
from glide.extensions import *


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


def test_process_pool_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(
        RowCSVExtractor("extract", nrows=10)
        | ProcessPoolPush("push", split=True)
        | [Printer("load1"), Printer("load2"), Printer("load3")]
    )
    glider.consume([infile])


def test_thread_pool_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(
        RowCSVExtractor("extract", nrows=10)
        | ThreadPoolPush("push")
        | [Printer("load1"), Printer("load2"), Printer("load3")]
    )
    glider.consume([infile])


def test_thread_pool_push_reduce(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(
        RowCSVExtractor("extract", nrows=10)
        | ThreadPoolPush("push", split=True)
        | [Printer("load1"), Printer("load2"), Printer("load3")]
        | ThreadReducer("reducer")
        | Printer("load4")
    )
    glider.consume([infile])
