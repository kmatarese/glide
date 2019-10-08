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
