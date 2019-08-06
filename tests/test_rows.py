from .test_utils import *

from consecutils import *

# -------- File-based pipelines


def test_csv_row_extract_and_load(rootdir):
    nodes = RowCSVExtractor("extract") | RowCSVLoader("load")
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        pipeline.consume([infile], load=dict(outfile=f))


def test_csv_chunked_row_extract_and_load(rootdir):
    nodes = RowCSVExtractor("extract") | RowCSVLoader("load")
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        pipeline.consume([infile], extract=dict(chunksize=100), load=dict(outfile=f))


def test_csv_row_process_pool_lowercase(rootdir):
    nodes = (
        RowCSVExtractor("extract")
        | RowProcessPoolTransformer("transform")
        | RowCSVLoader("load")
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        pipeline.consume([infile], transform=dict(func=row_lower), load=dict(outfile=f))


def test_csv_row_thread_pool_lowercase(rootdir):
    nodes = (
        RowCSVExtractor("extract")
        | RowThreadPoolTransformer("transform")
        | RowCSVLoader("load")
    )
    pipeline, infile, outfile = file_pipeline(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        pipeline.consume([infile], transform=dict(func=row_lower), load=dict(outfile=f))


# -------- SQL-based pipelines


def test_sql_row_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = RowSQLExtractor("extract") | RowSQLLoader("load")
    pipeline, table = sqlite_pipeline(rootdir, nodes, reset_output=True)
    sql = "select * from %s limit 10" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()
    pipeline.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn),
        load=dict(conn=sqlite_out_conn, table=table),
    )
    sqlite_out_conn.commit()


def test_sqlite_row_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = RowSQLiteExtractor("extract") | RowSQLiteLoader("load")
    pipeline, table = sqlite_pipeline(rootdir, nodes, reset_output=True)
    sql = "select * from %s limit 10" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()
    pipeline.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn),
        load=dict(conn=sqlite_out_conn, table=table),
    )
    sqlite_out_conn.commit()


def test_pymysql_row_extract_and_load(rootdir, pymysql_conn):
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn, truncate=True)
    sql = "select * from %s limit 10" % in_table
    pipeline = Consecutor(RowSQLDBAPIExtractor("extract") | RowSQLDBAPILoader("load"))
    pipeline.consume(
        [sql],
        extract=dict(conn=pymysql_conn, cursor=cursor),
        load=dict(conn=pymysql_conn, cursor=cursor, table=out_table),
    )


def test_sqlalchemy_row_extract_and_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn, truncate=True)
    sql = "select * from %s limit 10" % in_table
    pipeline = Consecutor(
        RowSQLAlchemyExtractor("extract") | RowSQLAlchemyLoader("load")
    )
    pipeline.consume(
        [sql],
        extract=dict(conn=sqlalchemy_conn),
        load=dict(conn=sqlalchemy_conn, table=out_table),
    )


def test_pymysql_table_extract(rootdir, pymysql_conn):
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn)
    pipeline = Consecutor(
        RowSQLDBAPITableExtractor("extract", limit=10) | LoggingLoader("load")
    )
    pipeline.consume([in_table], extract=dict(conn=pymysql_conn, cursor=cursor))


def test_sqlalchemy_table_extract(rootdir, sqlalchemy_conn):
    in_table, _ = sqlalchemy_setup(rootdir, sqlalchemy_conn)
    pipeline = Consecutor(
        RowSQLAlchemyTableExtractor("extract", limit=10) | LoggingLoader("load")
    )
    pipeline.consume([in_table], extract=dict(conn=sqlalchemy_conn))


def test_sqlite_row_temp_load(rootdir, sqlite_in_conn):
    nodes = (
        RowSQLiteExtractor("extract")
        | RowSQLiteTempLoader("tmp_loader")
        | LoggingLoader("load")
    )
    pipeline, table = sqlite_pipeline(rootdir, nodes)
    sql = "select * from %s limit 10" % table
    pipeline.consume(
        [sql], extract=dict(conn=sqlite_in_conn), tmp_loader=dict(conn=sqlite_in_conn)
    )


def test_sql_row_temp_load(rootdir, sqlite_in_conn):
    nodes = (
        RowSQLExtractor("extract")
        | RowSQLTempLoader("tmp_loader")
        | LoggingLoader("load")
    )
    pipeline, table = sqlite_pipeline(rootdir, nodes)
    sql = "select * from %s limit 10" % table
    pipeline.consume(
        [sql], extract=dict(conn=sqlite_in_conn), tmp_loader=dict(conn=sqlite_in_conn)
    )


def test_sql_chunked_row_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = RowSQLExtractor("extract") | RowSQLLoader("load")
    pipeline, table = sqlite_pipeline(rootdir, nodes, reset_output=True)
    sql = "select * from %s limit 100" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()
    pipeline.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn, chunksize=25),
        load=dict(conn=sqlite_out_conn, table=table),
    )
    sqlite_out_conn.commit()
