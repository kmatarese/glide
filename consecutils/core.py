from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from inspect import signature, Parameter

try:
    from dask import compute, delayed
    from dask.distributed import Client, as_completed as dask_as_completed
except ImportError:
    compute = None
    delayed = None
    Client = None
    dask_as_completed = None
import numpy as np
import sqlite3

from consecution import (
    Pipeline,
    GlobalState as ConsecutionGlobalState,
    Node as ConsecutionNode,
)
from consecutils.sql_utils import (
    SQLALCHEMY_CONN_TYPES,
    is_sqlalchemy_conn,
    get_bulk_replace,
)
from consecutils.utils import iterize, set_missing_key, MappingMixin


class GlobalState(MappingMixin, ConsecutionGlobalState):
    # Get this to behave more like a dict
    def __bool__(self):
        # Hack to get Consecution to use this as default even if technically empty
        return True


class Node(ConsecutionNode):
    def __init__(self, name, **default_context):
        super().__init__(name)
        self.default_context = default_context
        self.reset_context()
        self.run_args, self.run_kwargs = self._get_run_args()

    def update_context(self, context):
        self.context.update(context)

    def reset_context(self):
        self.context = self.default_context.copy()

    def _get_run_args(self):
        positionals = OrderedDict()
        keywords = OrderedDict()
        sig = signature(self.run)

        for i, param_name in enumerate(sig.parameters):
            param = sig.parameters[param_name]
            if i == 0:
                # The first param is the item to process which is passed
                # directly in process()
                continue
            if param.kind == param.POSITIONAL_ONLY:
                positionals[param.name] = None
            elif (
                param.default == Parameter.empty
                and param.kind == param.POSITIONAL_OR_KEYWORD
            ):
                positionals[param.name] = None
            elif param.kind == param.POSITIONAL_OR_KEYWORD:
                keywords[param.name] = param.default
            elif param.kind == param.VAR_KEYWORD:
                pass
            else:
                assert False, "%s params are not allowed in run()" % param.kind

        return positionals, keywords

    def _populate_run_args(self):
        _args = []
        for run_arg in self.run_args:
            if run_arg not in self.context:
                if self.global_state and run_arg in self.global_state:
                    # Use global_state as a backup for populating positional args
                    _args.append(self.global_state[run_arg])
                else:
                    raise Exception(
                        'Required run arg "%s" is missing from context: %s'
                        % (run_arg, self.context)
                    )
            else:
                _args.append(self.context[run_arg])

        # Everything else in the node context will be passed as part of kwargs
        # if it hasn't already been used in run_args
        _kwargs = {}
        for key in self.context:
            if key in self.run_args:
                continue
            _kwargs[key] = self.context[key]

        return _args, _kwargs

    def process(self, item):
        _args, _kwargs = self._populate_run_args()
        self.run(iterize(item), *_args, **_kwargs)

    def _run(self, item, *args, **kwargs):
        self.run(item, *args, **kwargs)

    def run(self, item, *args, **kwargs):
        raise NotImplementedError


class DefaultNode(Node):
    def run(self, item, **kwargs):
        self.push(item)


class PlaceholderNode(DefaultNode):
    pass


class SkipFalseNode(Node):
    """This overrides the behavior of calling run() such that if a "false"
    object is pushed it will never call run, just push to next node instead"""

    def _run(self, item, *args, **kwargs):
        if not item:
            self.push(item)
            return
        self.run(item, *args, **kwargs)


class DataFramePushMixin:
    def do_push(self, df, **kwargs):
        if kwargs.get("chunksize", None):
            for chunk in df:
                self.push(chunk)
        else:
            self.push(df)


class SQLCursorPushMixin:
    def do_push(self, cursor, chunksize=None):
        if chunksize:
            while True:
                chunk = cursor.fetchmany(chunksize)
                if not chunk:
                    break
                self.push(chunk)
        else:
            data = cursor.fetchall()
            self.push(data)


class DataFramePushNode(Node, DataFramePushMixin):
    pass


class BaseSQLConnectionNode(SkipFalseNode):
    allowed_conn_types = None

    def __init__(self, *args, **kwargs):
        assert self.allowed_conn_types and isinstance(
            self.allowed_conn_types, (list, tuple)
        ), (
            "%s.allowed_conn_types must be a list or tuple of connection types"
            % self.__class__.__name__
        )
        super().__init__(*args, **kwargs)

    def begin(self):
        conn = self.context.get("conn", None) or self.global_state.get("conn", None)
        assert conn, (
            "%s requires a conn argument in context or global state"
            % self.__class__.__name__
        )
        self.check_conn(conn)

    def _is_allowed_conn(self, conn):
        return isinstance(conn, tuple(self.allowed_conn_types))

    def check_conn(self, conn):
        assert self._is_allowed_conn(conn), (
            "Connection type %s is not in allowed types: %s"
            % (type(conn), self.allowed_conn_types)
        )


class PandasSQLConnectionNode(BaseSQLConnectionNode, DataFramePushMixin):
    allowed_conn_types = SQLALCHEMY_CONN_TYPES + [sqlite3.Connection]


class SQLAlchemyConnectionNode(BaseSQLConnectionNode, SQLCursorPushMixin):
    allowed_conn_types = SQLALCHEMY_CONN_TYPES


class SQLiteConnectionNode(BaseSQLConnectionNode, SQLCursorPushMixin):
    allowed_conn_types = [sqlite3.Connection]


class SQLDBAPIConnectionNode(BaseSQLConnectionNode, SQLCursorPushMixin):
    allowed_conn_types = [object]

    def check_conn(self, conn):
        super().check_conn(conn)
        assert hasattr(conn, "cursor"), "DBAPI connections must have a cursor() method"


class SQLConnectionNode(BaseSQLConnectionNode, SQLCursorPushMixin):
    """A generic SQL node that will behave differently based on the conn type"""

    allowed_conn_types = [object]

    def check_conn(self, conn):
        assert hasattr(conn, "cursor") or is_sqlalchemy_conn(
            conn
        ), "Connection must have a cursor() method or be a SQLAlchemy connection"

    def get_sql_executor(self, conn):
        """Get the object that can execute queries"""
        if is_sqlalchemy_conn(conn):
            return conn
        return conn.cursor()

    def sql_execute(self, conn, cursor, sql, **kwargs):
        """Executes the sql statement and returns an object that can fetch results"""
        if is_sqlalchemy_conn(conn):
            qr = conn.execute(sql, **kwargs)
            return qr
        qr = cursor.execute(sql, **kwargs)
        return cursor

    def sql_executemany(self, conn, cursor, sql, rows):
        """Executes the sql statement and returns an object that can fetch results"""
        if is_sqlalchemy_conn(conn):
            qr = conn.execute(sql, rows)
            return qr
        qr = cursor.executemany(sql, rows)
        return cursor

    def get_bulk_replace(self, conn, table, rows):
        if is_sqlalchemy_conn(conn):
            return get_bulk_replace(table, rows[0].keys(), dicts=False)
        if isinstance(conn, sqlite3.Connection):
            assert isinstance(
                rows[0], sqlite3.Row
            ), "Only sqlite3.Row rows are supported"
            return get_bulk_replace(
                table, rows[0].keys(), dicts=False, value_string="?"
            )
        return get_bulk_replace(table, rows[0].keys())


class Reducer(Node):
    def begin(self):
        self.results = []

    def run(self, item, **kwargs):
        self.results.append(item)

    def end(self):
        self.push(self.results)


class ThreadReducer(Reducer):
    pass


class FuturesPushNode(DefaultNode):
    executor_class = ProcessPoolExecutor
    as_completed_func = as_completed

    def _push(self, item):
        if self._logging == "output":
            self._write_log(item)

        executor_kwargs = self.context.get("executor_kwargs", None) or {}
        with self.executor_class(**executor_kwargs) as executor:
            futures = []

            if self.context.get("split", False):
                splits = np.array_split(item, len(self._downstream_nodes))
                for i, downstream in enumerate(self._downstream_nodes):
                    futures.append(executor.submit(downstream._process, splits[i]))
            else:
                for downstream in self._downstream_nodes:
                    futures.append(executor.submit(downstream._process, item))

            for future in self.__class__.as_completed_func(futures):
                result = future.result()


class ProcessPoolPush(FuturesPushNode):
    pass


class ThreadPoolPush(FuturesPushNode):
    executor_class = ThreadPoolExecutor


class DaskClientPush(FuturesPushNode):
    executor_class = Client
    as_completed_func = dask_as_completed

    def run(self, *args, **kwargs):
        assert Client, "Please install dask (Client) to use DaskClientPush"
        super().run(*args, **kwargs)


class DaskDelayedPush(DefaultNode):
    def _push(self, item):
        assert delayed, "Please install dask (delayed) to use DaskDelayedPush"

        if self._logging == "output":
            self._write_log(item)

        assert not "executor_kwargs" in self.context, (
            "%s does not currently support executor_kwargs" % self.__class__
        )

        lazy = []
        if self.context.get("split", False):
            splits = np.array_split(item, len(self._downstream_nodes))
            for i, downstream in enumerate(self._downstream_nodes):
                lazy.append(delayed(downstream._process)(split[i]))
        else:
            for downstream in self._downstream_nodes:
                lazy.append(delayed(downstream._process)(item))
        result = compute(lazy)


def update_node_contexts(pipeline, node_contexts):
    for k, v in node_contexts.items():
        assert k in pipeline._node_lookup, "Invalid node: %s" % k
        pipeline[k].update_context(v)


def reset_node_contexts(pipeline, node_contexts):
    for k in node_contexts:
        assert k in pipeline._node_lookup, "Invalid node: %s" % k
        pipeline[k].reset_context()


def consume(pipeline, data, **node_contexts):
    update_node_contexts(pipeline, node_contexts)
    pipeline.consume(data)
    reset_node_contexts(pipeline, node_contexts)


class Consecutor:
    def __init__(self, *args, **kwargs):
        # NOTE: We can't subclass Pipeline due to a bug in pickle that screws
        # up the super().func reference when using multiprocessing, causing
        # infinite recursion
        set_missing_key(
            kwargs, "global_state", GlobalState()
        )  # Ensure our version is default
        self.pipeline = Pipeline(*args, **kwargs)

    def __getitem__(self, name):
        return self.pipeline[name]

    def __setitem__(self, name_to_replace, replacement_node):
        self.pipeline[name_to_replace] = replacement_node

    def __str__(self):
        return self.pipeline.__str__()

    def consume(self, data, **node_contexts):
        consume(self.pipeline, data, **node_contexts)

    def plot(self, *args, **kwargs):
        self.pipeline.plot(*args, **kwargs)


class DaskParacutor(Consecutor):
    def consume(self, data, **node_contexts):
        assert Client, "Please install dask (Client) to use DaskParacutor"

        with Client() as client:  # Local multi-processor for now
            splits = np.array_split(data, min(len(data), len(client.ncores())))
            futures = []
            for split in splits:
                futures.append(
                    client.submit(consume, self.pipeline, splits, **node_contexts)
                )
            for future in dask_as_completed(futures):
                result = future.result()


class ProcessPoolParacutor(Consecutor):
    def consume(self, data, **node_contexts):
        with ProcessPoolExecutor() as executor:
            splits = np.array_split(data, min(len(data), executor._max_workers))
            futures = []
            for split in splits:
                futures.append(
                    executor.submit(consume, self.pipeline, split, **node_contexts)
                )
            for future in as_completed(futures):
                result = future.result()


class ThreadPoolParacutor(Consecutor):
    def consume(self, data, **node_contexts):
        with ThreadPoolExecutor() as executor:
            splits = np.array_split(data, min(len(data), executor._max_workers))
            futures = []
            for split in splits:
                futures.append(
                    executor.submit(consume, self.pipeline, split, **node_contexts)
                )
            for future in as_completed(futures):
                result = future.result()
