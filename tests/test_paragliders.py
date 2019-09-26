from .test_utils import *
from glide import *
from glide.extensions import *


def test_csv_dask_paraglider(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = DaskParaGlider(RowCSVExtractor("extract") | Printer("load"))
    glider.consume([infile], extract=dict(nrows=10))


def test_csv_process_pool_paraglider(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = ProcessPoolParaGlider(RowCSVExtractor("extract") | Printer("load"))
    glider.consume([infile], extract=dict(nrows=10))


def test_csv_thread_pool_paraglider(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = ThreadPoolParaGlider(RowCSVExtractor("extract") | Printer("load"))
    glider.consume([infile], extract=dict(nrows=10))


def test_sql_process_pool_paraglider(rootdir):
    in_table, out_table = dbapi_tables()
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = ProcessPoolParaGlider(RowSQLExtractor("extract") | PrettyPrinter("load"))
    glider.consume(
        [sql],
        extract=dict(
            conn=ContextFunc(get_pymysql_conn),
            cursor_type=pymysql.cursors.DictCursor,
            params=dict(zip="01000"),
        ),
    )


def test_sql_dataframe_process_pool_paraglider(rootdir):
    in_table, out_table = dbapi_tables()
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = ProcessPoolParaGlider(
        DataFrameSQLExtractor("extract") | PrettyPrinter("load")
    )
    glider.consume(
        [sql],
        extract=dict(conn=ContextFunc(get_pymysql_conn), params=dict(zip="01000")),
    )


def test_sql_thread_pool_paraglider(rootdir):
    in_table, out_table = dbapi_tables()
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = ThreadPoolParaGlider(RowSQLExtractor("extract") | PrettyPrinter("load"))
    glider.consume(
        [sql],
        extract=dict(
            conn=ContextFunc(get_pymysql_conn),
            cursor_type=pymysql.cursors.DictCursor,
            params=dict(zip="01000"),
        ),
    )
