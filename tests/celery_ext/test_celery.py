from functools import partial

from celery.contrib.testing import worker
import pytest
from tlbx import st

from .app import (
    app,
    lower_task,
    celery_build_glider_task,
    celery_consume_task,
    celery_glider_task,
    celery_glider_template_task,
)
from glide import *
from glide.extensions.celery import *
from ..test_utils import *


@pytest.fixture(scope="session")
def celery_enable_logging():
    return True


@pytest.fixture(scope="session")
def celery_app():
    return app


@pytest.fixture(scope="session")
def celery_includes():
    return ["celery.app"]


@pytest.fixture(scope="session")
def celery_worker_pool():
    return "solo"


# For debugging
# @pytest.fixture(scope="session")
# def celery_worker_parameters():
#     return dict(
#         logfile='/tmp/worker.log',
#         loglevel='info'
#     )


def get_async_result(async_result):
    try:
        result = async_result.get(propagate=False)
        assert async_result.successful(), "Task failed: %s\n%s" % (
            async_result.info,
            async_result.traceback,
        )
    finally:
        async_result.forget()
    return result


def send_celery_task(name, *args, **kwargs):
    async_result = app.send_task(name, args=args, kwargs=kwargs)
    return get_async_result(async_result)


def test_lower(redis_server, celery_worker):
    # XXX: sometimes celery registers it as just app.lower_task?
    # result = send_celery_task('app.lower_task', [dict(x="TEST")])
    result = send_celery_task("tests.celery_ext.app.lower_task", [dict(x="TEST")])
    dbg(result)


def test_celery_task_node(redis_server, celery_worker, rootdir):
    # Note: have to wait for result for celery_worker fixtures to work properly
    nodes = (
        CSVExtract("extract", nrows=20)
        | CeleryApplyAsync("apply", task=lower_task, push_type="result", retry=False)
        | Print("load")
    )
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(nodes)
    glider.consume([infile])


def test_celery_reducer(redis_server, celery_worker, rootdir):
    nodes = (
        CSVExtract("extract", nrows=20)
        | SplitPush("split", split_count=2)
        | CeleryApplyAsync("apply", task=lower_task)
        | CeleryReduce("reduce")
        | Print("load")
    )
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(nodes)
    glider.consume([infile])


def test_celery_glider_task(redis_server, celery_worker, rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = Glider(CSVExtract("extract") | [Print("load1"), Print("load2")])
    async_result = celery_glider_task.delay(
        [infile], glider, consume_kwargs=dict(extract=dict(nrows=10))
    )
    # Note: have to wait for result for celery_worker fixtures to work properly
    result = get_async_result(async_result)


def test_celery_build_glider_task(redis_server, celery_worker, rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    async_result = celery_build_glider_task.delay(
        [infile],
        [
            dict(class_name="CSVExtract", args=["extract"]),
            [
                dict(class_name="Print", args=["load1"]),
                dict(class_name="Print", args=["load2"]),
            ],
        ],
        consume_kwargs=dict(extract=dict(nrows=10)),
    )
    # Note: have to wait for result for celery_worker fixtures to work properly
    result = get_async_result(async_result)


def test_celery_glider_template_task(redis_server, celery_worker, rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider_template = GliderTemplate(
        partial(CSVExtract, "extract"),
        [partial(Print, "load1"), partial(Print, "load2")],
    )
    async_result = celery_glider_template_task.delay(
        [infile], glider_template, consume_kwargs=dict(extract=dict(nrows=10))
    )
    # Note: have to wait for result for celery_worker fixtures to work properly
    result = get_async_result(async_result)


def test_celery_paraglider_csv(redis_server, celery_worker, rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = CeleryParaGlider(
        celery_consume_task, CSVExtract("extract") | Print("load")
    )
    async_results = glider.consume([infile], extract=dict(nrows=10))
    # Note: have to wait for result for celery_worker fixtures to work properly
    for async_result in async_results:
        result = get_async_result(async_result)


def test_celery_glider_sql(redis_server, celery_worker, rootdir):
    in_table, out_table = db_tables()
    sql = "select * from %s where Zip_Code < %%(zip)s" % in_table

    glider = Glider(SQLExtract("extract") | PrettyPrint("load"))

    async_result = celery_glider_task.delay(
        [sql],
        glider,
        consume_kwargs=dict(
            cleanup=dict(extract_conn=closer),
            extract=dict(conn=RuntimeContext(get_sqlalchemy_conn), zip="01000"),
        ),
    )

    result = get_async_result(async_result)
