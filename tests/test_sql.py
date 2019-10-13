from .test_utils import *
from glide import *


def test_sql_row_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = RowSQLExtractor("extract") | RowSQLLoader("load")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s limit 10" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()
    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn),
        load=dict(conn=sqlite_out_conn, table=table, stmt_type="INSERT"),
    )
    sqlite_out_conn.commit()


def test_sql_row_param_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = RowSQLParamExtractor("extract", log=True) | RowSQLLoader("load")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s where Zip_Code = :zip" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()

    param_data = [dict(zip="00501"), dict(zip="00544"), dict(zip="01001")]
    glider.consume(
        param_data,
        extract=dict(sql=sql, conn=sqlite_in_conn),
        load=dict(conn=sqlite_out_conn, table=table),
    )
    sqlite_out_conn.commit()


def test_sqlite_row_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = RowSQLExtractor("extract") | RowSQLLoader("load")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s where Zip_Code < :zip" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()
    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn, params=dict(zip="01000")),
        load=dict(conn=sqlite_out_conn, table=table),
    )
    sqlite_out_conn.commit()


def test_pymysql_row_extract_and_load(rootdir, pymysql_conn):
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn, truncate=True)
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = Glider(RowSQLExtractor("extract") | RowSQLLoader("load"))
    glider.consume(
        [sql],
        extract=dict(conn=pymysql_conn, cursor=cursor, params=dict(zip="01000")),
        load=dict(
            conn=pymysql_conn,
            cursor=cursor,
            table=out_table,
            stmt_type="INSERT",
            odku=True,
        ),
    )


def test_sqlalchemy_row_extract_and_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn, truncate=True)
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = Glider(RowSQLExtractor("extract") | RowSQLLoader("load"))
    glider.consume(
        [sql],
        extract=dict(conn=sqlalchemy_conn, zip="01000"),
        load=dict(conn=sqlalchemy_conn, table=out_table),
    )


def test_pymysql_table_extract(rootdir, pymysql_conn):
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn)
    glider = Glider(RowSQLTableExtractor("extract", limit=10) | Printer("load"))
    glider.consume([in_table], extract=dict(conn=pymysql_conn, cursor=cursor))


def test_sqlalchemy_table_extract(rootdir, sqlalchemy_conn):
    in_table, _ = sqlalchemy_setup(rootdir, sqlalchemy_conn)
    glider = Glider(
        RowSQLTableExtractor("extract", limit=10)
        | FormatPrinter("load", indent="label", label="node")
    )
    glider.consume([in_table], extract=dict(conn=sqlalchemy_conn))


def test_sqlite_row_temp_load(rootdir, sqlite_in_conn):
    nodes = (
        RowSQLExtractor("extract") | RowSQLTempLoader("tmp_loader") | Printer("load")
    )
    glider, table = sqlite_glider(rootdir, nodes)
    sql = "select * from %s limit 10" % table
    glider.consume(
        [sql], extract=dict(conn=sqlite_in_conn), tmp_loader=dict(conn=sqlite_in_conn)
    )


def test_sql_row_temp_load(rootdir, sqlite_in_conn):
    nodes = (
        RowSQLExtractor("extract") | RowSQLTempLoader("tmp_loader") | Printer("load")
    )
    glider, table = sqlite_glider(rootdir, nodes)
    sql = "select * from %s where Zip_Code < :zip" % table
    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn, params=dict(zip="01000")),
        tmp_loader=dict(conn=sqlite_in_conn),
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
