import pytest

from .test_utils import *
from glide import *


def test_sql_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = SQLExtract("extract") | SQLLoad("load")
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


def test_sql_execute_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = SQLExecute("extract") | SQLFetch("fetch") | SQLLoad("load")
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


def test_sqlite_swap_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = SQLExtract("extract") | SQLLoad("load")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s where Zip_Code < :zip" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()
    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn, params=dict(zip="01000")),
        load=dict(conn=sqlite_out_conn, table=table, swap=True),
    )
    sqlite_out_conn.commit()


def test_pymysql_swap_load(rootdir, pymysql_conn):
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn, truncate=True)
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = Glider(SQLExtract("extract") | SQLLoad("load"))
    glider.consume(
        [sql],
        extract=dict(conn=pymysql_conn, cursor=cursor, params=dict(zip="01000")),
        load=dict(
            conn=pymysql_conn,
            cursor=cursor,
            table=out_table,
            stmt_type="INSERT",
            odku=True,
            swap=True,
        ),
    )


def test_sqlalchemy_swap_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn, truncate=True)
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = Glider(SQLExtract("extract") | SQLLoad("load"))
    glider.consume(
        [sql],
        extract=dict(conn=sqlalchemy_conn, zip="01000"),
        load=dict(conn=sqlalchemy_conn, table=out_table, swap=True),
    )


def test_sqlite_tx_rollback(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = SQLExtract("extract") | SQLTransaction("tx") | SQLLoad("load")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s where Zip_Code < :zip" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()
    with pytest.raises(sqlite3.OperationalError):
        glider.consume(
            [sql],
            extract=dict(conn=sqlite_in_conn, params=dict(zip="01000")),
            tx=dict(conn=sqlite_out_conn),
            load=dict(
                conn=sqlite_out_conn, table=table + "foo", rollback=True  # Cause error
            ),
        )


def test_sql_pymysql_tx_rollback(rootdir, pymysql_conn):
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn, truncate=True)
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = Glider(SQLExtract("extract") | SQLTransaction("tx") | SQLLoad("load"))

    with pytest.raises(pymysql.err.ProgrammingError):
        glider.consume(
            [sql],
            extract=dict(conn=pymysql_conn, cursor=cursor, params=dict(zip="01000")),
            tx=dict(conn=pymysql_conn),
            load=dict(
                conn=pymysql_conn,
                cursor=cursor,
                table=out_table + "foo",  # Cause error
                rollback=True,
            ),
        )


def test_sqlalchemy_tx_rollback(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn, truncate=True)
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = Glider(SQLExtract("extract") | SQLTransaction("tx") | SQLLoad("load"))

    with pytest.raises(sa.exc.ProgrammingError):
        glider.consume(
            [sql],
            extract=dict(conn=sqlalchemy_conn, zip="01000"),
            tx=dict(conn=sqlalchemy_conn),
            load=dict(
                conn=sqlalchemy_conn,
                table=out_table + "foo",  # Cause error
                rollback=True,
            ),
        )


def test_sql_assert_node(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = SQLExtract("extract") | SQLLoad("load") | AssertSQL("check")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s limit 10" % table
    assert_sql = "select (select count(*) as x from %s) == 10 as assert" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()

    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn),
        load=dict(conn=sqlite_out_conn, table=table, stmt_type="INSERT"),
        check=dict(conn=sqlite_out_conn, sql=assert_sql),
    )
    sqlite_out_conn.commit()


def test_sql_assert_data_check(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = SQLExtract("extract") | SQLLoad("load") | AssertSQL("check")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s limit 10" % table
    assert_sql = "select count(*) as assert from %s" % table
    sqlite_out_conn.execute("delete from %s" % table)
    sqlite_out_conn.commit()

    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn),
        load=dict(
            conn=sqlite_out_conn, table=table, push_data=True, stmt_type="INSERT"
        ),
        check=dict(
            conn=sqlite_out_conn,
            sql=assert_sql,
            data_check=lambda node, data: len(data),
        ),
    )
    sqlite_out_conn.commit()


def test_sql_param_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = SQLParamExtract("extract", _log=True) | SQLLoad("load")
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


def test_sqlite_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = SQLExtract("extract") | SQLLoad("load")
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


def test_pymysql_extract_and_load(rootdir, pymysql_conn):
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn, truncate=True)
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = Glider(SQLExtract("extract") | SQLLoad("load"))
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


def test_sqlalchemy_extract_and_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn, truncate=True)
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = Glider(SQLExtract("extract") | SQLLoad("load"))
    glider.consume(
        [sql],
        extract=dict(conn=sqlalchemy_conn, zip="01000"),
        load=dict(conn=sqlalchemy_conn, table=out_table),
    )


def test_pymysql_table_extract(rootdir, pymysql_conn):
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn)
    glider = Glider(SQLTableExtract("extract", limit=10) | Print("load"))
    glider.consume([in_table], extract=dict(conn=pymysql_conn, cursor=cursor))


def test_sqlalchemy_table_extract(rootdir, sqlalchemy_conn):
    in_table, _ = sqlalchemy_setup(rootdir, sqlalchemy_conn)
    glider = Glider(
        SQLTableExtract("extract", limit=10)
        | FormatPrint("load", indent="label", label="node")
    )
    glider.consume([in_table], extract=dict(conn=sqlalchemy_conn))


def test_sqlite_temp_load(rootdir, sqlite_in_conn):
    nodes = SQLExtract("extract") | SQLTempLoad("tmp_loader") | Print("load")
    glider, table = sqlite_glider(rootdir, nodes)
    sql = "select * from %s limit 10" % table
    glider.consume(
        [sql], extract=dict(conn=sqlite_in_conn), tmp_loader=dict(conn=sqlite_in_conn)
    )


def test_sql_temp_load(rootdir, sqlite_in_conn):
    nodes = SQLExtract("extract") | SQLTempLoad("tmp_loader") | Print("load")
    glider, table = sqlite_glider(rootdir, nodes)
    sql = "select * from %s where Zip_Code < :zip" % table
    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn, params=dict(zip="01000")),
        tmp_loader=dict(conn=sqlite_in_conn),
    )


def test_sql_chunked_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = SQLExtract("extract") | SQLLoad("load")
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
