import time

import pytest
from pytest_redis import factories
from redis import Redis
from redis.exceptions import ConnectionError
from rq import Queue
from tlbx import st
from xprocess import ProcessStarter

from glide import *
from glide.extensions.rq import *
from ..conftest import noop_fixture
from ..test_utils import *


# Hack: allows tests to run in CI environment too
if redis_running():
    print("External Redis server detected")
    redis_server = noop_fixture
else:
    print("Starting Redis server")
    redis_server = factories.redis_proc(
        executable=test_config.get("RedisExecutable", "/usr/bin/redis-server"),
        host=test_config.get("RedisHost", "localhost"),
        port=test_config.get("RedisPort", 6379),
    )
    time.sleep(0.5)


@pytest.fixture(scope="session")
def rq_worker(xprocess):
    class Starter(ProcessStarter):
        pattern = "Listening on"
        args = ["rq", "worker", "--with-scheduler"]

    name = "rq_worker"
    print("Ensuring RQ worker...")
    logfile = xprocess.ensure(name, Starter)
    yield
    print("Stopping RQ worker.")
    xprocess.getinfo(name).terminate()


def test_rq_job_node(redis_server, rq_worker, rootdir):
    nodes = (
        CSVExtract("extract", nrows=20, _log=True)
        | RQJob("apply", func=lower_rows, push_type="result", timeout=5)
        | Print("load")
    )
    infile, _ = get_filenames(rootdir, "csv")
    redis_conn = Redis()
    glider = Glider(nodes)
    glider.consume([infile], apply=dict(redis_conn=redis_conn))


def test_rq_reducer(redis_server, rq_worker, rootdir):
    nodes = (
        CSVExtract("extract", nrows=20)
        | SplitPush("split", split_count=4)
        | RQJob("apply", func=lower_rows)
        | RQReduce("reduce")
        | Print("load")
    )
    infile, _ = get_filenames(rootdir, "csv")
    redis_conn = Redis()
    glider = Glider(nodes)
    glider.consume([infile], apply=dict(redis_conn=redis_conn))


def test_rq_paraglider_csv(redis_server, rq_worker, rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    redis_conn = Redis()
    queue = Queue(connection=redis_conn)
    glider = RQParaGlider(queue, CSVExtract("extract") | Print("load"))
    async_results = glider.consume([infile], extract=dict(nrows=10))
    results = get_async_results(async_results)
