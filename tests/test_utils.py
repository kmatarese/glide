import configparser
import logging
import os
from shutil import copyfile
import sqlite3

try:
    import pymysql
except ImportError:
    pymysql = None
try:
    from redis import Redis
    from redis.exceptions import ConnectionError
except:
    Redis = None
import sqlalchemy as sa
from tlbx import st, rmfile

from glide import *
from glide.utils import dbg, info, warn, error

logging.getLogger("glide").setLevel(logging.INFO)

TEST_DATA_NAME = "dma_zip"

assert "GLIDE_CONFIG_FILE" in os.environ, (
    "Please specify the location of a glide config file using the environment "
    "variable GLIDE_CONFIG_FILE"
)
config = configparser.ConfigParser()
config.read(os.environ["GLIDE_CONFIG_FILE"])
test_config = config["TEST"]


def redis_running():
    conn = Redis(socket_connect_timeout=1)
    try:
        conn.ping()
        return True
    except ConnectionError:
        return False


def lower(s):
    return s.lower() if type(s) == str else s


def df_lower(df):
    df = df.applymap(lower)
    return df


def row_lower(row):
    for k, v in row.items():
        row[k] = lower(v)
    return row


def lower_rows(data):
    for row in data:
        row_lower(row)
    return data


def get_filenames(rootdir, extension):
    infile = "%s/%s.%s" % (rootdir, TEST_DATA_NAME, extension)
    outfile = "%s/%s.%s" % (test_config["OutputDirectory"], TEST_DATA_NAME, extension)
    rmfile(outfile, ignore_missing=True)
    return infile, outfile


def get_db_filenames(rootdir):
    table = TEST_DATA_NAME
    db_name = "%s.db" % table
    in_db_file = rootdir + "/" + db_name
    out_db_file = "%s/%s" % (test_config["OutputDirectory"], db_name)
    return table, in_db_file, out_db_file


def copy_sqlite_test_db():
    rootdir = get_current_dir()
    table, in_db_file, out_db_file = get_db_filenames(rootdir)
    copyfile(in_db_file, out_db_file)


def clear_sqlite_table_if_exists(conn, table):
    try:
        conn.execute("delete from %s" % table)
        conn.commit()
    except sqlite3.OperationalError:
        pass


def get_sqlalchemy_mysql_engine():
    host = test_config["MySQLHost"]
    port = int(test_config["MySQLPort"])
    user = test_config["MySQLUser"]
    password = test_config.get("MySQLPassword", None)
    schema = test_config["MySQLTestSchema"]
    if host in ["localhost", "127.0.0.1"] and not password:
        conn_str = "mysql+pymysql://%(user)s@%(host)s/%(schema)s" % locals()
    else:
        conn_str = (
            "mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(schema)s"
            % locals()
        )
    engine = sa.create_engine(conn_str)
    return engine


def get_sqlalchemy_conn():
    engine = get_sqlalchemy_mysql_engine()
    return engine.connect()


def get_pymysql_conn():
    assert pymysql, "PyMySQL package is not installed"
    host = test_config["MySQLHost"]
    port = int(test_config["MySQLPort"])
    user = test_config["MySQLUser"]
    password = test_config.get("MySQLPassword", None)
    schema = test_config["MySQLTestSchema"]
    conn = pymysql.connect(
        host=host,
        port=port,
        db=schema,
        user=user,
        passwd=password,
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn


def get_sqlite_in_conn():
    rootdir = get_current_dir()
    _, in_db_file, _ = get_db_filenames(rootdir)
    conn = sqlite3.connect(in_db_file)
    conn.row_factory = sqlite3.Row
    return conn


def get_sqlite_out_conn():
    rootdir = get_current_dir()
    _, _, out_db_file = get_db_filenames(rootdir)
    conn = sqlite3.connect(out_db_file)
    conn.row_factory = sqlite3.Row
    return conn


def sqlite_glider(rootdir, nodes, reset_output=False):
    """Note: this should not be called once you have already connected to
    the sqlite output DB"""
    table, in_db_file, out_db_file = get_db_filenames(rootdir)
    if reset_output:
        rmfile(out_db_file, ignore_missing=True)
        copyfile(in_db_file, out_db_file)
    glider = Glider(nodes)
    return glider, table


def sqlalchemy_setup(rootdir, conn, truncate=False, sa_objects=False):
    in_table, out_table = db_tables()
    if truncate:
        try:
            conn.execute("truncate %s" % out_table)
        except Exception as e:
            print("Error during truncation: %s" % str(e))

    if sa_objects:
        meta = sa.MetaData()
        meta.bind = conn.engine
        meta.reflect()
        in_table = meta.tables[in_table.split(".")[-1]]
        out_table = meta.tables[out_table.split(".")[-1]]

    return in_table, out_table


def db_tables():
    in_table = "%s.%s" % (test_config["MySQLTestSchema"], TEST_DATA_NAME)
    out_table = "%s_tmp" % in_table
    return in_table, out_table


def dbapi_setup(rootdir, conn, truncate=False):
    in_table, out_table = db_tables()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("use %s" % test_config["MySQLTestSchema"])
    if truncate:
        cursor.execute("drop table if exists %s" % out_table)
        cursor.execute("create table %s like %s" % (out_table, in_table))
    return in_table, out_table, cursor


def file_glider(rootdir, extension, nodes):
    infile, outfile = get_filenames(rootdir, extension)
    glider = Glider(nodes)
    return glider, infile, outfile


def get_current_dir():
    return os.path.dirname(os.path.abspath(__file__))
