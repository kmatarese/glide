import pytest
from redis import Redis
from rq import Queue
from tlbx import st
from xprocess import ProcessStarter

from glide import *
from glide.extensions.rq import *
from ..test_utils import *


@pytest.fixture(scope="session")
def rq_worker(xprocess):
    class Starter(ProcessStarter):
        pattern = "Listening on"
        args = ["rq", "worker"]

    name = "rq_worker"
    logfile = xprocess.ensure(name, Starter)
    yield
    print("Stopping RQ worker.")
    xprocess.getinfo(name).terminate()


def test_rq_job_node(redis_server, rq_worker, rootdir):
    nodes = (
        RowCSVExtractor("extract", nrows=20)
        | RQJob("apply", func=lower_rows, push_type="result")
        | Printer("load")
    )
    infile, _ = get_filenames(rootdir, "csv")
    redis_conn = Redis()
    glider = Glider(nodes)
    glider.consume([infile], apply=dict(redis_conn=redis_conn))


def test_rq_reducer(redis_server, rq_worker, rootdir):
    nodes = (
        RowCSVExtractor("extract", nrows=20)
        | SplitPush("split", split_count=4)
        | RQJob("apply", func=lower_rows)
        | RQReducer("reduce")
        | Printer("load")
    )
    infile, _ = get_filenames(rootdir, "csv")
    redis_conn = Redis()
    glider = Glider(nodes)
    glider.consume([infile], apply=dict(redis_conn=redis_conn))


def test_rq_paraglider_csv(redis_server, rq_worker, rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    redis_conn = Redis()
    queue = Queue(connection=redis_conn)
    glider = RQParaGlider(queue, RowCSVExtractor("extract") | Printer("load"))
    async_results = glider.consume([infile], extract=dict(nrows=10))
    results = get_async_results(async_results)
