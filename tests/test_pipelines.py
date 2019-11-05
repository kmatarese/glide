from .test_utils import *
from glide import *


def test_sql2sql(rootdir, pymysql_conn):
    glider = SQL2SQL()
    in_table, out_table, cursor = dbapi_setup(rootdir, pymysql_conn, truncate=True)
    sql = "select * from %s limit 10" % in_table
    glider.consume(
        [sql],
        extract=dict(conn=pymysql_conn, cursor=cursor),
        load=dict(conn=pymysql_conn, cursor=cursor, table=out_table),
    )


def test_parallel_csv2csv(rootdir):
    glider = CSV2CSV(glider=ProcessPoolParaGlider)
    infile, outfile = get_filenames(rootdir, "csv")
    glider.consume([infile], load=dict(f=outfile))


def test_node_template(rootdir):
    nodes = CSVExtract("extract", chunksize=10, nrows=20) | Print("load")
    temp = NodeTemplate(nodes)
    inst = temp()
    glider = Glider(inst)
    infile, outfile = get_filenames(rootdir, "csv")
    glider.consume([infile])
