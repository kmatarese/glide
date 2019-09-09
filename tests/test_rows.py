from .test_utils import *
from glide import *

# -------- File-based gliders


def test_csv_url_row_extract_and_load(rootdir):
    nodes = RowCSVExtractor("extract") | RowCSVLoader("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    infile = "file://" + infile
    with open(outfile, "w") as f:
        glider.consume([infile], load=dict(f=f))


def test_csv_row_extract_and_load(rootdir):
    nodes = RowCSVExtractor("extract") | RowCSVLoader("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], load=dict(f=f))


def test_csv_chunked_row_extract_and_load(rootdir):
    nodes = RowCSVExtractor("extract") | RowCSVLoader("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], extract=dict(chunksize=100), load=dict(f=f))


def test_csv_row_process_pool_lowercase(rootdir):
    nodes = (
        RowCSVExtractor("extract")
        | RowProcessPoolTransformer("transform")
        | RowCSVLoader("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], transform=dict(func=row_lower), load=dict(f=f))


def test_csv_row_thread_pool_lowercase(rootdir):
    nodes = (
        RowCSVExtractor("extract")
        | RowThreadPoolTransformer("transform")
        | RowCSVLoader("load")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], transform=dict(func=row_lower), load=dict(f=f))


# -------- SQL-based gliders


def test_sql_row_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = RowSQLExtractor("extract") | RowSQLLoader("load")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s limit 10" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()
    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn),
        load=dict(conn=sqlite_out_conn, table=table),
    )
    sqlite_out_conn.commit()


def test_sqlite_row_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = RowSQLiteExtractor("extract") | RowSQLiteLoader("load")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s limit 10" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()
    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn),
        load=dict(conn=sqlite_out_conn, table=table),
    )
    sqlite_out_conn.commit()


def test_pymysql_row_extract_and_load(rootdir, pymysql_conn):
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn, truncate=True)
    sql = "select * from %s limit 10" % in_table
    glider = Glider(RowSQLDBAPIExtractor("extract") | RowSQLDBAPILoader("load"))
    glider.consume(
        [sql],
        extract=dict(conn=pymysql_conn, cursor=cursor),
        load=dict(conn=pymysql_conn, cursor=cursor, table=out_table),
    )


def test_sqlalchemy_row_extract_and_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn, truncate=True)
    sql = "select * from %s limit 10" % in_table
    glider = Glider(RowSQLAlchemyExtractor("extract") | RowSQLAlchemyLoader("load"))
    glider.consume(
        [sql],
        extract=dict(conn=sqlalchemy_conn),
        load=dict(conn=sqlalchemy_conn, table=out_table),
    )


def test_pymysql_table_extract(rootdir, pymysql_conn):
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn)
    glider = Glider(
        RowSQLDBAPITableExtractor("extract", limit=10) | LoggingLoader("load")
    )
    glider.consume([in_table], extract=dict(conn=pymysql_conn, cursor=cursor))


def test_sqlalchemy_table_extract(rootdir, sqlalchemy_conn):
    in_table, _ = sqlalchemy_setup(rootdir, sqlalchemy_conn)
    glider = Glider(
        RowSQLAlchemyTableExtractor("extract", limit=10) | LoggingLoader("load")
    )
    glider.consume([in_table], extract=dict(conn=sqlalchemy_conn))


def test_sqlite_row_temp_load(rootdir, sqlite_in_conn):
    nodes = (
        RowSQLiteExtractor("extract")
        | RowSQLiteTempLoader("tmp_loader")
        | LoggingLoader("load")
    )
    glider, table = sqlite_glider(rootdir, nodes)
    sql = "select * from %s limit 10" % table
    glider.consume(
        [sql], extract=dict(conn=sqlite_in_conn), tmp_loader=dict(conn=sqlite_in_conn)
    )


def test_sql_row_temp_load(rootdir, sqlite_in_conn):
    nodes = (
        RowSQLExtractor("extract")
        | RowSQLTempLoader("tmp_loader")
        | LoggingLoader("load")
    )
    glider, table = sqlite_glider(rootdir, nodes)
    sql = "select * from %s limit 10" % table
    glider.consume(
        [sql], extract=dict(conn=sqlite_in_conn), tmp_loader=dict(conn=sqlite_in_conn)
    )


def test_sql_chunked_row_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = RowSQLExtractor("extract") | RowSQLLoader("load")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s limit 100" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()
    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn, chunksize=25),
        load=dict(conn=sqlite_out_conn, table=table),
    )
    sqlite_out_conn.commit()
