"""
Core classes used to power pipelines
"""
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
from glide.sql_utils import (
    SQLALCHEMY_CONN_TYPES,
    is_sqlalchemy_conn,
    get_bulk_replace,
)
from glide.utils import st, iterize, set_missing_key, MappingMixin, is_pandas


class GlobalState(MappingMixin, ConsecutionGlobalState):
    """Consecution GlobalState with more dict-like behavior"""
    def __bool__(self):
        """Hack to get Consecution to use this as default even if technically empty"""
        return True


class Node(ConsecutionNode):
    """Override Consecution's Node class to add necessary functionality"""
    def __init__(self, name, **default_context):
        super().__init__(name)
        self.default_context = default_context
        self.reset_context()
        self.run_args, self.run_kwargs = self._get_run_args()

    def update_context(self, context):
        """Update the context dict for this Node"""
        self.context.update(context)

    def reset_context(self):
        """Reset context dict for this Node to the default"""
        self.context = self.default_context.copy()

    def _get_run_args(self):
        """Get the args and kwargs of this Node's run() method"""
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
        """Populate the args to run() based on the current context"""
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
        """Required method used by Consecution to process nodes"""
        _args, _kwargs = self._populate_run_args()
        self._run(iterize(item), *_args, **_kwargs)

    def _run(self, item, *args, **kwargs):
        self.run(item, *args, **kwargs)

    def run(self, item, *args, **kwargs):
        """Subclasses will override this method to implement core node logic """
        raise NotImplementedError


class DefaultNode(Node):
    """A default node that just passes all items through"""
    def run(self, item, **kwargs):
        self.push(item)


class PlaceholderNode(DefaultNode):
    """Used as a placeholder in pipelines. Will pass values through by default"""
    pass


class SkipFalseNode(Node):
    """This overrides the behavior of calling run() such that if a "false"
    object is pushed it will never call run, just push to next node instead"""

    def _run(self, item, *args, **kwargs):
        if is_pandas(item):
            if item.empty:
                self.push(item)
                return
        else:
            if not item:
                self.push(item)
                return
        self.run(item, *args, **kwargs)


class DataFramePushMixin:
    """Shared logic for DataFrame-based nodes"""
    def do_push(self, df, **kwargs):
        """Push the DataFrame to the next node, obeying chunksize if passed"""
        if kwargs.get("chunksize", None):
            for chunk in df:
                self.push(chunk)
        else:
            self.push(df)


class SQLCursorPushMixin:
    """Shared logic for SQL cursor-based nodes"""
    def do_push(self, cursor, chunksize=None):
        """Fetch data and push to the next node, obeying chunksize if passed"""
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
    """Base class for DataFrame-based nodes"""
    pass


class BaseSQLConnectionNode(SkipFalseNode):
    """Base class for SQL-based nodes, checks for valid connection types on init"""
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
    """Captures the connection types allowed to work with Pandas to_sql/from_sql"""
    allowed_conn_types = SQLALCHEMY_CONN_TYPES + [sqlite3.Connection]


class SQLAlchemyConnectionNode(BaseSQLConnectionNode, SQLCursorPushMixin):
    """Captures the connection types allowed to work with SQLAlchemy"""
    allowed_conn_types = SQLALCHEMY_CONN_TYPES


class SQLiteConnectionNode(BaseSQLConnectionNode, SQLCursorPushMixin):
    """Captures the connection types allowed to work with SQLite"""
    allowed_conn_types = [sqlite3.Connection]


class SQLDBAPIConnectionNode(BaseSQLConnectionNode, SQLCursorPushMixin):
    """Checks that a valid DBAPI connection is passed"""
    allowed_conn_types = [object]

    def check_conn(self, conn):
        super().check_conn(conn)
        assert hasattr(conn, "cursor"), "DBAPI connections must have a cursor() method"


class SQLConnectionNode(BaseSQLConnectionNode, SQLCursorPushMixin):
    """A generic SQL node that will behave differently based on the conn type"""

    allowed_conn_types = [object]

    def check_conn(self, conn):
        """Make sure the object is a valid SQL connection"""
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
        """Bulk executes the sql statement and returns an object that can fetch results"""
        if is_sqlalchemy_conn(conn):
            qr = conn.execute(sql, rows)
            return qr
        qr = cursor.executemany(sql, rows)
        return cursor

    def get_bulk_replace(self, conn, table, rows):
        """Get a bulk replace SQL statement"""
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
    """Waits until end() to call push(), effectively waiting for all nodes before
    it to finish before continuing the pipeline"""

    def begin(self):
        """Setup a place for results to be collected"""
        self.results = []

    def run(self, item, **kwargs):
        """Collect results from previous nodes"""
        self.results.append(item)

    def end(self):
        """Do the push once all results are in"""
        self.push(self.results)


class ThreadReducer(Reducer):
    """A plain-old Reducer with a name that makes it clear it works with threads"""
    pass


class FuturesPushNode(DefaultNode):
    """A node that either splits or duplicates its input to pass to multiple
    downstream nodes in parallel according to the executor_class that supports
    the futures interface.
    """
    executor_class = ProcessPoolExecutor
    as_completed_func = as_completed

    def _push(self, item):
        """Override Consecution's push such that we can push in parallel"""
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
    """A multi-process FuturesPushNode"""
    pass


class ThreadPoolPush(FuturesPushNode):
    """A multi-threaded FuturesPushNode"""
    executor_class = ThreadPoolExecutor


class DaskClientPush(FuturesPushNode):
    """Use a dask Client to do a parallel push"""
    executor_class = Client
    as_completed_func = dask_as_completed

    def run(self, *args, **kwargs):
        assert Client, "Please install dask (Client) to use DaskClientPush"
        super().run(*args, **kwargs)


class DaskDelayedPush(DefaultNode):
    """Use dask delayed to do a parallel push"""
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
    """Helper function for updating node contexts in a pipeline"""
    for k, v in node_contexts.items():
        assert k in pipeline._node_lookup, "Invalid node: %s" % k
        pipeline[k].update_context(v)


def reset_node_contexts(pipeline, node_contexts):
    """Helper function for resetting node contexts in a pipeline"""
    for k in node_contexts:
        assert k in pipeline._node_lookup, "Invalid node: %s" % k
        pipeline[k].reset_context()


def consume(pipeline, data, **node_contexts):
    """Handles node contexts before/after calling pipeline.consume()"""
    update_node_contexts(pipeline, node_contexts)
    pipeline.consume(data)
    reset_node_contexts(pipeline, node_contexts)


class Glider:
    """Main class for forming and executing pipelines. It thinly wraps
    Consecution's Pipeline, but does not subclass it due to a bug in pickle
    that hits an infinite recursion when using multiprocessing with a
    super().func reference.
    """

    def __init__(self, *args, **kwargs):
        """Initialize the pipeline"""
        set_missing_key(
            kwargs, "global_state", GlobalState()
        )  # Ensure our version is default
        self.pipeline = Pipeline(*args, **kwargs)

    def __getitem__(self, name):
        """Passthrough to Consecution Pipeline"""
        return self.pipeline[name]

    def __setitem__(self, name_to_replace, replacement_node):
        """Passthrough to Consecution Pipeline"""
        self.pipeline[name_to_replace] = replacement_node

    def __str__(self):
        """Passthrough to Consecution Pipeline"""
        return self.pipeline.__str__()

    def consume(self, data, **node_contexts):
        """Setup node contexts and consume data with the pipeline"""
        consume(self.pipeline, data, **node_contexts)

    def plot(self, *args, **kwargs):
        """Passthrough to Consecution Pipeline"""
        self.pipeline.plot(*args, **kwargs)


class DaskParaGlider(Glider):
    """A parallel Glider that uses a dask Client to execute parallel calls to
    consume()"""

    def consume(self, data, **node_contexts):
        """Setup node contexts and consume data with the pipeline"""
        assert Client, "Please install dask (Client) to use DaskParaGlider"

        with Client() as client:  # Local multi-processor for now
            splits = np.array_split(data, min(len(data), len(client.ncores())))
            futures = []
            for split in splits:
                futures.append(
                    client.submit(consume, self.pipeline, splits, **node_contexts)
                )
            for future in dask_as_completed(futures):
                result = future.result()


class ProcessPoolParaGlider(Glider):
    """A parallel Glider that uses a ProcessPoolExecutor to execute parallel calls to
    consume()"""

    def consume(self, data, **node_contexts):
        """Setup node contexts and consume data with the pipeline"""
        with ProcessPoolExecutor() as executor:
            splits = np.array_split(data, min(len(data), executor._max_workers))
            futures = []
            for split in splits:
                futures.append(
                    executor.submit(consume, self.pipeline, split, **node_contexts)
                )
            for future in as_completed(futures):
                result = future.result()


class ThreadPoolParaGlider(Glider):
    """A parallel Glider that uses a ThreadPoolExecutor to execute parallel calls to
    consume()"""

    def consume(self, data, **node_contexts):
        """Setup node contexts and consume data with the pipeline"""
        with ThreadPoolExecutor() as executor:
            splits = np.array_split(data, min(len(data), executor._max_workers))
            futures = []
            for split in splits:
                futures.append(
                    executor.submit(consume, self.pipeline, split, **node_contexts)
                )
            for future in as_completed(futures):
                result = future.result()
