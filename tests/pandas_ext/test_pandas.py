from ..test_utils import *
from glide import *
from glide.extensions.pandas import *

# -------- File-based gliders


def test_dataframe_csv_extract_and_load(rootdir):
    nodes = DataFrameCSVExtract("extract") | DataFrameCSVLoad(
        "load", index=False, mode="a"
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile], extract=dict(chunksize=100), load=dict(f=outfile))


def test_dataframe_excel_extract_and_load(rootdir):
    nodes = DataFrameExcelExtract("extract") | DataFrameExcelLoad("load", index=False)
    glider, infile, outfile = file_glider(rootdir, "xlsx", nodes)
    glider.consume([infile], load=dict(f=outfile))


def test_dataframe_chunked_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtract("extract")
        | DataFrameApplyMap("transform")
        | DataFrameCSVLoad("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume(
        [infile],
        extract=dict(chunksize=100),
        transform=dict(func=lower),
        load=dict(f=outfile),
    )


def test_dataframe_process_pool_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtract("extract")
        | ProcessPoolSubmit("transform", push_type=PushTypes.Result)
        | DataFrameCSVLoad("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile], transform=dict(func=df_lower), load=dict(f=outfile))


def test_dataframe_thread_pool_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtract("extract")
        | ThreadPoolSubmit(
            "transform", executor_kwargs=dict(max_workers=4), push_type=PushTypes.Result
        )
        | DataFrameCSVLoad("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume([infile], transform=dict(func=df_lower), load=dict(f=outfile))


# -------- SQL-based gliders


def test_dataframe_sqlite_extract_and_load(rootdir, sqlite_in_conn, sqlite_out_conn):
    nodes = DataFrameSQLExtract("extract") | DataFrameSQLLoad("load")
    glider, table = sqlite_glider(rootdir, nodes, reset_output=True)
    sql = "select * from %s where Zip_Code < :zip" % table
    glider.consume(
        [sql],
        extract=dict(conn=sqlite_in_conn, params=dict(zip="01000")),
        load=dict(table=table, conn=sqlite_out_conn),
    )


def test_dataframe_sqlalchemy_temp_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn)
    sql = "select * from %s limit 10" % in_table
    glider = Glider(
        DataFrameSQLExtract("extract")
        | DataFrameSQLTempLoad("tmp_loader")
        | Print("load")
    )
    glider.consume(
        [sql], extract=dict(conn=sqlalchemy_conn), tmp_loader=dict(conn=sqlalchemy_conn)
    )


def test_dataframe_sqlalchemy_extract_and_load(rootdir, sqlalchemy_conn):
    in_table, out_table = sqlalchemy_setup(rootdir, sqlalchemy_conn, truncate=True)
    sql = "select * from %s limit 100" % in_table
    glider = Glider(
        DataFrameSQLExtract("extract")
        | DataFrameSQLLoad("load", if_exists="replace", index=False)
    )
    glider.consume(
        [sql],
        extract=dict(conn=sqlalchemy_conn),
        load=dict(table=out_table, conn=sqlalchemy_conn),
    )


def test_dataframe_sqlalchemy_table_extract(rootdir, sqlalchemy_conn):
    in_table, _ = sqlalchemy_setup(rootdir, sqlalchemy_conn)
    glider = Glider(DataFrameSQLTableExtract("extract", limit=100) | Print("load"))
    glider.consume([in_table], extract=dict(conn=sqlalchemy_conn))


def test_dataframe_sql_process_pool_paraglider(rootdir):
    in_table, out_table = db_tables()
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table
    glider = ProcessPoolParaGlider(DataFrameSQLExtract("extract") | PrettyPrint("load"))
    glider.consume(
        [sql],
        synchronous=True,
        cleanup=dict(extract_conn=closer),
        timeout=5,
        extract=dict(
            conn=RuntimeContext(get_sqlalchemy_conn), params=dict(zip="01000")
        ),
    )
