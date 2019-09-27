from .test_utils import *
from glide import *
from glide.extensions import *

# -------- File-based gliders


def test_placeholder_node(rootdir):
    nodes = PlaceholderNode("extract") | DataFrameCSVLoader(
        "load", index=False, mode=RuntimeContext(lambda: "a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider["extract"] = DataFrameCSVExtractor("extract")
    glider.consume([infile], extract=dict(chunksize=100), load=dict(f=outfile))


def test_csv_extract_and_load(rootdir):
    nodes = DataFrameCSVExtractor("extract") | DataFrameCSVLoader(
        "load", index=False, mode="a"
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile], extract=dict(chunksize=100), load=dict(f=outfile))


def test_excel_extract_and_load(rootdir):
    nodes = DataFrameExcelExtractor("extract") | DataFrameExcelLoader(
        "load", index=False
    )
    glider, infile, outfile = file_glider(rootdir, "xlsx", nodes)
    glider.consume([infile], load=dict(f=outfile))


def test_csv_chunked_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DataFrameApplyMapTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume(
        [infile],
        extract=dict(chunksize=100),
        transform=dict(func=lower),
        load=dict(f=outfile),
    )


def test_csv_chunked_swifter_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | SwifterApplyTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume(
        [infile],
        extract=dict(chunksize=500),
        transform=dict(func=lower),
        load=dict(f=outfile),
    )


def test_csv_chunked_swifter_threads_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | SwifterApplyTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume(
        [infile],
        extract=dict(chunksize=500),
        transform=dict(func=lower, processes=False),
        load=dict(f=outfile),
    )


def test_csv_process_pool_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DataFrameProcessPoolTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile], transform=dict(func=df_lower), load=dict(f=outfile))


def test_csv_thread_pool_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DataFrameThreadPoolTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile], transform=dict(func=df_lower), load=dict(f=outfile))


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


# -------- SQL-based gliders


def test_sqlite_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = DataFrameSQLExtractor("extract") | DataFrameSQLLoader("load")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s where Zip_Code < :zip" % table
    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn, params=dict(zip="01000")),
        load=dict(table=table, conn=sqlite_out_conn),
    )


def test_sqlalchemy_temp_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn)
    sql = "select * from %s limit 10" % in_table
    glider = Glider(
        DataFrameSQLExtractor("extract")
        | DataFrameSQLTempLoader("tmp_loader")
        | Printer("load")
    )
    glider.consume(
        [sql], extract=dict(conn=sqlalchemy_conn), tmp_loader=dict(conn=sqlalchemy_conn)
    )


def test_sqlalchemy_extract_and_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn, truncate=True)
    sql = "select * from %s limit 100" % in_table
    glider = Glider(
        DataFrameSQLExtractor("extract")
        | DataFrameSQLLoader("load", if_exists="replace", index=False)
    )
    glider.consume(
        [sql],
        extract=dict(conn=sqlalchemy_conn),
        load=dict(table=out_table, conn=sqlalchemy_conn),
    )


def test_sqlalchemy_table_extract(rootdir, sqlalchemy_conn):
    in_table, _ = sqlalchemy_setup(rootdir, sqlalchemy_conn)
    glider = Glider(DataFrameSQLTableExtractor("extract", limit=100) | Printer("load"))
    glider.consume([in_table], extract=dict(conn=sqlalchemy_conn))
