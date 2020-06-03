import pytest

from .test_utils import *


@pytest.fixture
def noop_fixture():
    pass


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
    conn = get_sqlite_in_conn()
    yield conn
    conn.close()


@pytest.fixture
def sqlite_out_conn():
    conn = get_sqlite_out_conn()
    yield conn
    conn.close()
