from .test_utils import *
from consecutils import *

# -------- File-based pipelines


def test_placeholder_node(rootdir):
    nodes = PlaceholderNode("extract") | DataFrameCSVLoader(
        "load", index=False, mode="a"
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    pipeline["extract"] = DataFrameCSVExtractor("extract")
    pipeline.consume([infile], extract=dict(chunksize=100), load=dict(outfile=outfile))


def test_csv_extract_and_load(rootdir):
    nodes = DataFrameCSVExtractor("extract") | DataFrameCSVLoader(
        "load", index=False, mode="a"
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    pipeline.consume([infile], extract=dict(chunksize=100), load=dict(outfile=outfile))


def test_excel_extract_and_load(rootdir):
    nodes = DataFrameExcelExtractor("extract") | DataFrameExcelLoader(
        "load", index=False
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "xlsx", nodes)
    pipeline.consume([infile], load=dict(outfile=outfile))


def test_csv_chunked_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DataFrameApplyMapTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    pipeline.consume(
        [infile],
        extract=dict(chunksize=100),
        transform=dict(func=lower),
        load=dict(outfile=outfile),
    )


def test_csv_chunked_swifter_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | SwifterApplyTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    pipeline.consume(
        [infile],
        extract=dict(chunksize=500),
        transform=dict(func=lower),
        load=dict(outfile=outfile),
    )


def test_csv_chunked_swifter_threads_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | SwifterApplyTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    pipeline.consume(
        [infile],
        extract=dict(chunksize=500),
        transform=dict(func=lower, processes=False),
        load=dict(outfile=outfile),
    )


def test_csv_process_pool_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DataFrameProcessPoolTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    pipeline.consume(
        [infile], transform=dict(func=df_lower), load=dict(outfile=outfile)
    )


def test_csv_thread_pool_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DataFrameThreadPoolTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    pipeline.consume(
        [infile], transform=dict(func=df_lower), load=dict(outfile=outfile)
    )


def test_csv_dask_client_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DataFrameDaskClientTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    pipeline.consume(
        [infile], transform=dict(func=df_lower), load=dict(outfile=outfile)
    )


def test_csv_dask_client_threads_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DataFrameDaskClientTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    pipeline.consume(
        [infile],
        transform=dict(func=df_lower, executor_kwargs=dict(processes=False)),
        load=dict(outfile=outfile),
    )


def test_csv_dask_dataframe_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | DaskDataFrameApplyTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    pipeline.consume([infile], transform=dict(func=lower), load=dict(outfile=outfile))


# -------- SQL-based pipelines


def test_sqlite_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = DataFrameSQLExtractor("extract") | DataFrameSQLLoader("load")
    pipeline, table = sqlite_pipeline(rootdir, nodes, reset_output=True)
    sql = "select * from %s limit 10" % table
    pipeline.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn),
        load=dict(table=table, conn=sqlite_out_conn),
    )


# def test_sqlite_temp_load(rootdir, sqlite_in_conn):
#     nodes = (DataFrameSQLExtractor('extract') |
#              DataFrameSQLTempLoader('tmp_loader') |
#              LoggingLoader('load'))
#     pipeline, table = sqlite_pipeline(rootdir, nodes)
#     sql = 'select * from %s limit 10' % table
#     pipeline.consume([sql],
#                      extract=dict(conn=sqlite_in_conn),
#                      tmp_loader=dict(conn=sqlite_in_conn))


def test_sqlalchemy_temp_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn)
    sql = "select * from %s limit 10" % in_table
    pipeline = Consecutor(
        DataFrameSQLExtractor("extract")
        | DataFrameSQLTempLoader("tmp_loader")
        | LoggingLoader("load")
    )
    pipeline.consume(
        [sql], extract=dict(conn=sqlalchemy_conn), tmp_loader=dict(conn=sqlalchemy_conn)
    )


def test_sqlalchemy_extract_and_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn, truncate=True)
    sql = "select * from %s limit 100" % in_table
    pipeline = Consecutor(
        DataFrameSQLExtractor("extract")
        | DataFrameSQLLoader("load", if_exists="replace", index=False)
    )
    pipeline.consume(
        [sql],
        extract=dict(conn=sqlalchemy_conn),
        load=dict(table=out_table, conn=sqlalchemy_conn),
    )


def test_sqlalchemy_table_extract(rootdir, sqlalchemy_conn):
    in_table, _ = sqlalchemy_setup(rootdir, sqlalchemy_conn)
    pipeline = Consecutor(
        DataFrameSQLTableExtractor("extract", limit=100) | LoggingLoader("load")
    )
    pipeline.consume([in_table], extract=dict(conn=sqlalchemy_conn))
