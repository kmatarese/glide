from unittest.mock import patch

import pytest

from .test_utils import get_pymysql_conn
from glide import *

BASE_ARGV = ["test_script_decorator.py"]
LOAD_TABLE = "scratch.dma_zip_tmp"

gs_glider = Glider(
    RowSQLExtractor("extract") | RowSQLLoader("load"),
    global_state=dict(conn=get_pymysql_conn()),
)

cf_glider = Glider(
    RowSQLExtractor("extract") | RowSQLLoader("load"),
    global_state=dict(conn=RuntimeContext(get_pymysql_conn)),
)

glider = Glider(RowSQLExtractor("extract") | RowSQLLoader("load"))


def get_data():
    return ["select * from scratch.dma_zip limit 10"]


def get_data_str():
    return " ".join(get_data())


def get_argv(*args, data=True):
    argv = BASE_ARGV[:]
    if data:
        argv.append(get_data_str())
    if len(args):
        argv.extend(*args)
    return argv


def get_load_table():
    return ["--load_table", LOAD_TABLE]


def get_conn():
    conn = get_pymysql_conn()


@gs_glider.cli()
def _test_base_cli(data, node_contexts):
    gs_glider.consume(data, **node_contexts)


def test_base_cli():
    with patch("argparse._sys.argv", get_argv(get_load_table())):
        _test_base_cli()


@Parent()
@Arg("--dry_run", action="store_true")
def parent_cli():
    pass


@gs_glider.cli(parents=[parent_cli])
def _test_parent_cli(data, dry_run, node_contexts):
    assert dry_run
    gs_glider.consume(data, **node_contexts)


def test_parent_cli():
    with patch("argparse._sys.argv", get_argv(get_load_table() + ["--dry_run"])):
        _test_parent_cli()


@cf_glider.cli(Arg("--load_table", required=False, default=LOAD_TABLE))
def _test_arg_override(data, node_contexts):
    gs_glider.consume(data, **node_contexts)


def test_arg_override():
    with patch("argparse._sys.argv", get_argv()):
        _test_arg_override()


@gs_glider.cli()
def _test_help(data, node_contexts):
    assert False


def test_help():
    with patch("argparse._sys.argv", get_argv(["--help"])):
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            _test_help()


@glider.cli(inject=dict(data=get_data, conn=RuntimeContext(get_pymysql_conn)))
def _test_injected_args(data, conn, node_contexts):
    glider.consume(data, clean=dict(conn=lambda x: x.close()), **node_contexts)


def test_injected_args():
    with patch("argparse._sys.argv", get_argv(get_load_table(), data=False)):
        _test_injected_args()


@glider.cli(
    inject=dict(
        data=get_data, extract_conn=get_pymysql_conn, load_conn=get_pymysql_conn
    ),
    clean=dict(extract_conn=lambda x: x.close(), load_conn=lambda x: x.close()),
)
def _test_injected_args_with_node_prefix(data, node_contexts, **kwargs):
    glider.consume(data, **node_contexts)


def test_injected_args_with_node_prefix():
    with patch("argparse._sys.argv", get_argv(get_load_table(), data=False)):
        _test_injected_args_with_node_prefix()
