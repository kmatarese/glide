from .test_utils import *

from consecutils import *


def test_dask_delayed_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    pipeline = Consecutor(
        RowCSVExtractor("extract", nrows=10)
        | DaskDelayedPush("push")
        | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
    )
    pipeline.consume([infile])


def test_dask_client_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    pipeline = Consecutor(
        RowCSVExtractor("extract", nrows=10)
        | DaskClientPush("push")
        | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
    )
    pipeline.consume([infile])


def test_process_pool_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    pipeline = Consecutor(
        RowCSVExtractor("extract", nrows=10)
        | ProcessPoolPush("push", split=True)
        | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
    )
    pipeline.consume([infile])


def test_thread_pool_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    pipeline = Consecutor(
        RowCSVExtractor("extract", nrows=10)
        | ThreadPoolPush("push")
        | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
    )
    pipeline.consume([infile])


def test_thread_pool_push_reduce(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    pipeline = Consecutor(
        RowCSVExtractor("extract", nrows=10)
        | ThreadPoolPush("push", split=True)
        | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
        | ThreadReducer("reducer")
        | LoggingLoader("load4")
    )
    pipeline.consume([infile])
