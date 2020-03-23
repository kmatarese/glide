import pytest
from pytest_redis import factories

from .test_utils import *


@pytest.fixture
def rootdir():
    return get_current_dir()


@pytest.fixture
def sqlalchemy_conn():
    engine = get_sqlalchemy_mysql_engine()
    conn = engine.connect()
    conn.execute("use %s" % test_config["MySQLTestSchema"])
    yield conn
    conn.close()


@pytest.fixture
def pymysql_conn():
    conn = get_pymysql_conn()
    yield conn
    conn.close()


@pytest.fixture
def sqlite_in_conn():
    rootdir = get_current_dir()
    _, in_db_file, _ = get_db_filenames(rootdir)
    conn = sqlite3.connect(in_db_file)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture
def sqlite_out_conn():
    rootdir = get_current_dir()
    _, _, out_db_file = get_db_filenames(rootdir)
    conn = sqlite3.connect(out_db_file)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


redis_server = factories.redis_proc(
    executable=test_config.get("RedisExecutable", "/usr/bin/redis-server"),
    host=test_config["RedisHost"],
    port=test_config["RedisPort"],
)
