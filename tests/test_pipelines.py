from .test_utils import *
from glide import *


def test_row_sql2sql(rootdir, pymysql_conn):
    glider = RowSQL2SQL()
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn, truncate=True)
    sql = "select * from %s limit 10" % in_table
    glider.consume(
        [sql],
        extract=dict(conn=pymysql_conn, cursor=cursor),
        load=dict(conn=pymysql_conn, cursor=cursor, table=out_table),
    )


def test_row_parallel_csv2csv(rootdir):
    glider = RowCSV2CSV(glider=ProcessPoolParaGlider)
    infile, outfile = get_filenames(rootdir, "csv")
    glider.consume([infile], load=dict(f=outfile))
