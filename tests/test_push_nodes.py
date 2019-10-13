from .test_utils import *
from glide import *


def test_iter_push():
    nodes = IterPush("push") | Printer("print")
    glider = Glider(nodes)
    glider.consume([range(4)])


def test_split_push():
    nodes = SplitPush("push", split_count=2) | Printer("print")
    glider = Glider(nodes)
    glider.consume([range(4)])


def test_split_by_node_push():
    nodes = SplitByNode("push") | [Printer("print1"), Printer("print2")]
    glider = Glider(nodes)
    glider.consume([range(6)])


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
        | ThreadReduce("reducer")
        | Printer("load4")
    )
    glider.consume([infile])
