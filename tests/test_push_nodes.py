from .test_utils import *
from glide import *


def test_iter_push():
    nodes = IterPush("push") | Print("print")
    glider = Glider(nodes)
    glider.consume([range(4)])


def test_split_push():
    nodes = SplitPush("push", split_count=2) | Print("print")
    glider = Glider(nodes)
    glider.consume([range(4)])


def test_split_by_node_push():
    nodes = SplitByNode("push") | [Print("print1"), Print("print2")]
    glider = Glider(nodes)
    glider.consume([range(6)])


def test_process_pool_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(
        CSVExtract("extract", nrows=10)
        | ProcessPoolPush("push", split=True)
        | [Print("load1"), Print("load2"), Print("load3")]
    )
    glider.consume([infile])


def test_thread_pool_push_node(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(
        CSVExtract("extract", nrows=10)
        | ThreadPoolPush("push")
        | [Print("load1"), Print("load2"), Print("load3")]
    )
    glider.consume([infile])


def test_thread_pool_push_reduce(rootdir):
    # This will push the same data to each logging node
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(
        CSVExtract("extract", nrows=10)
        | ThreadPoolPush("push", split=True)
        | [Print("load1"), Print("load2"), Print("load3")]
        | ThreadReduce("reducer")
        | Print("load4")
    )
    glider.consume([infile])
