from unittest.mock import patch

import pytest

from .test_utils import get_pymysql_conn, pymysql
from glide import *
from glide.utils import date_window_cli, datetime_window_cli

BASE_ARGV = ["test_script_decorator.py"]
LOAD_TABLE = "scratch.dma_zip_tmp"


class NoDataNode(NoInputNode):
    def run(self):
        self.push("Test!")


global_conn = get_pymysql_conn()

gs_glider = Glider(
    SQLExtract("extract") | SQLLoad("load"), global_state=dict(conn=global_conn)
)

rc_glider = Glider(
    SQLExtract("extract") | SQLLoad("load"),
    global_state=dict(conn=RuntimeContext(get_pymysql_conn)),
)

glider = Glider(SQLExtract("extract") | SQLLoad("load"))

no_data_glider = Glider(NoDataNode("extract") | Print("load"))


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
def _test_base_cli(glide_data, node_contexts):
    gs_glider.consume(glide_data, **node_contexts)


def test_base_cli():
    with patch("argparse._sys.argv", get_argv(get_load_table())):
        _test_base_cli()


@Parent()
@Arg("--dry_run", action="store_true")
def parent_cli():
    pass


@gs_glider.cli(parents=[parent_cli])
def _test_parent_cli(glide_data, dry_run, node_contexts):
    assert dry_run
    gs_glider.consume(glide_data, **node_contexts)


def test_parent_cli():
    with patch("argparse._sys.argv", get_argv(get_load_table() + ["--dry_run"])):
        _test_parent_cli()


@gs_glider.cli(parents=[date_window_cli])
def _test_date_window_cli(glide_data, date_windows, node_contexts, **kwargs):
    dbg(date_windows)


def test_date_window_cli():
    with patch("argparse._sys.argv", get_argv(get_load_table() + ["--yesterday"])):
        _test_date_window_cli()


@gs_glider.cli(parents=[datetime_window_cli])
def _test_datetime_window_cli(glide_data, date_windows, node_contexts, **kwargs):
    dbg(date_windows)


def test_datetime_window_cli():
    with patch(
        "argparse._sys.argv",
        get_argv(get_load_table() + ["--days-back", "1", "--date-window-size", "4"]),
    ):
        _test_datetime_window_cli()


@rc_glider.cli(Arg("--load_table", required=False, default=LOAD_TABLE))
def _test_arg_override(glide_data, node_contexts):
    rc_glider.consume(glide_data, **node_contexts)


def test_arg_override():
    with patch("argparse._sys.argv", get_argv()):
        _test_arg_override()


@rc_glider.cli(Arg("--table", dest="load_table", required=True))
def _test_arg_dest(glide_data, node_contexts):
    rc_glider.consume(glide_data, **node_contexts)


def test_arg_dest():
    with patch("argparse._sys.argv", get_argv(["--table", LOAD_TABLE])):
        _test_arg_dest()


@gs_glider.cli(
    Arg("--load_table", required=False, default=LOAD_TABLE, help="custom help")
)
def _test_help(glide_data, node_contexts):
    assert False


def test_help():
    with patch("argparse._sys.argv", get_argv(["--help"])):
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            _test_help()


@gs_glider.cli(
    Arg("--load_table", required=False, default=LOAD_TABLE, help="custom help")
)
def _test_no_commit(glide_data, node_contexts):
    gs_glider.consume(glide_data, **node_contexts)


def test_no_commit():
    with patch("argparse._sys.argv", get_argv(["--load_no_commit"])):
        _test_no_commit()
    # This commit is necessary to prevent some other tests that reuse this
    # conn from hanging.
    global_conn.commit()


@glider.cli(inject=dict(glide_data=get_data, conn=RuntimeContext(get_pymysql_conn)))
def _test_injected_args(glide_data, conn, node_contexts):
    glider.consume(glide_data, cleanup=dict(conn=lambda x: x.close()), **node_contexts)


def test_injected_args():
    with patch("argparse._sys.argv", get_argv(get_load_table(), data=False)):
        _test_injected_args()


@glider.cli(
    inject=dict(
        glide_data=get_data,
        extract_conn=get_pymysql_conn,
        load_conn=get_pymysql_conn,
        cursor_type=pymysql.cursors.DictCursor,
    ),
    cleanup=dict(extract_conn=lambda x: x.close(), load_conn=lambda x: x.close()),
)
def _test_injected_args_with_node_prefix(glide_data, node_contexts, **kwargs):
    glider.consume(glide_data, **node_contexts)


def test_injected_args_with_node_prefix():
    with patch("argparse._sys.argv", get_argv(get_load_table(), data=False)):
        _test_injected_args_with_node_prefix()


@no_data_glider.cli()
def _test_no_data_cli(node_contexts):
    no_data_glider.consume(**node_contexts)


def test_no_data_cli():
    with patch("argparse._sys.argv"):
        _test_no_data_cli()


@gs_glider.cli(Arg("--extract", type=str, default=None))
def _test_node_name_arg_name(glide_data, node_contexts):
    gs_glider.consume(glide_data, **node_contexts)


def test_node_name_arg_name():
    with patch("argparse._sys.argv", get_argv(get_load_table())):
        with pytest.raises(AssertionError):
            _test_node_name_arg_name()


def test_conflicting_dest_settings():
    with pytest.raises(AssertionError):

        @gs_glider.cli(
            Arg("--chunksize", type=int, default=10),
            Arg("--extract_chunksize", type=int, default=15),
        )
        def _test(glide_data, node_contexts):
            gs_glider.consume(glide_data, **node_contexts)


def test_conflicting_inject_args():
    with pytest.raises(AssertionError):

        @gs_glider.cli(
            Arg("--chunksize", type=int, default=5), inject=dict(chunksize=10)
        )
        def _test(glide_data, node_contexts, chunksize=None):
            gs_glider.consume(glide_data, **node_contexts)
