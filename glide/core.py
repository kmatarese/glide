"""Core classes used to power pipelines"""

from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import copy
from inspect import signature, Parameter
from pprint import pformat

import climax
import numpy as np
import sqlite3
from tlbx import st, Script, Arg, Parent, MappingMixin, set_missing_key, format_msg

from consecution import (
    Pipeline,
    GlobalState as ConsecutionGlobalState,
    Node as ConsecutionNode,
)
from glide.sql_utils import (
    SQLALCHEMY_CONN_TYPES,
    is_sqlalchemy_conn,
    get_bulk_statement,
)
from glide.utils import dbg, repr, iterize, is_pandas, closer

SCRIPT_DATA_ARG = "data"


class GlobalState(MappingMixin, ConsecutionGlobalState):
    """Consecution GlobalState with more dict-like behavior"""

    def __bool__(self):
        """Hack to get Consecution to use this as default even if technically empty"""
        return True


class Node(ConsecutionNode):
    """Override Consecution's Node class to add necessary functionality

    Parameters
    ----------
    name : str
        The name of the Node.
    log : bool, optional
        If true, log items processed by the node.
    **default_context
        Keyword args that establish the default_context of the Node.

    Attributes
    ----------
    name : str
        The name of the Node.
    log : bool
        If true, log items processed by the node. Note that this overrides
        Consecution's log() functionality.
    default_context : dict
        A dictionary to establish default context for the node that can be
        used to populate run() arguments.
    context : dict
        The current context of the Node
    run_args : dict
        An OrderedDict of positional args to run()
    run_kwargs : dict
        An OrderedDict of keyword args and defaults to run()

    """

    def __init__(self, name, log=False, **default_context):
        super().__init__(name)
        self.log = log
        self.default_context = default_context or {}
        self.reset_context()
        self.run_args, self.run_kwargs = self._get_run_args()

    def update_context(self, context):
        """Update the context dict for this Node"""
        self.context.update(context)

    def reset_context(self):
        """Reset context dict for this Node to the default"""
        self.context = copy.deepcopy(self.default_context)

    def _begin(self):
        for k, v in self.context.items():
            if isinstance(v, RuntimeContext):
                self.context[k] = v()
        super()._begin()

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

    def _get_run_arg_values(self):
        """Get the args to run() based on the current context"""
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
        arg_values, kwarg_values = self._get_run_arg_values()
        if self.log:
            print(format_msg(repr(item), label=self.name))
        else:
            dbg(repr(item), label=self.name)
        self._run(item, *arg_values, **kwarg_values)

    def _run(self, item, *args, **kwargs):
        self.run(item, *args, **kwargs)

    def run(self, item, *args, **kwargs):
        """Subclasses will override this method to implement core node logic"""
        raise NotImplementedError


class DefaultNode(Node):
    """A default node that just passes all items through"""

    def run(self, item, **kwargs):
        self.push(item)


class IterPushNode(Node):
    """A node that pushes each item of an iterable individually"""

    def run(self, data, **kwargs):
        for row in data:
            self.push(row)


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

    def do_push(self, df, chunksize=None):
        """Push the DataFrame to the next node, obeying chunksize if passed

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to push, or chunks of a DataFrame if the chunksize
            argument is passed and truthy.
        chunksize : int, optional
            If truthy the df argument is expected to be chunks of a DataFrame
            that will be pushed individually.
        """

        if chunksize:
            for chunk in df:
                self.push(chunk)
        else:
            self.push(df)


class SQLCursorPushMixin:
    """Shared logic for SQL cursor-based nodes"""

    def do_push(self, cursor, chunksize=None):
        """Fetch data and push to the next node, obeying chunksize if passed

        Parameters
        ----------
        cursor
            A cursor-like object with fetchmany and fetchall methods
        chunksize : int, optional
            If truthy the data will be fetched and pushed in chunks
        """
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


class BaseSQLNode(SkipFalseNode):
    """Base class for SQL-based nodes, checks for valid connection types on init

    Attributes
    ----------
    allowed_conn_types : list or tuple
        A list or tuple of connection types that are allowed
    """

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

    def get_sql_executor(self, conn, cursor_type=None):
        """Get the object that can execute queries"""
        if is_sqlalchemy_conn(conn):
            return conn
        return conn.cursor(cursor_type) if cursor_type else conn.cursor()

    def sql_execute(self, conn, cursor, sql, params=None, **kwargs):
        """Executes the sql statement and returns an object that can fetch results

        Parameters
        ----------
        conn
            A SQL database connection object
        cursor
            A SQL database cursor
        sql : str
            A sql query to execute
        params : tuple, optional
            A tuple of params to pass to the execute method of the conn or cursor
        **kwargs
            kwargs passed through to execute()

        Returns
        -------
        cursor
            cursor object that has executed but not fetched a query.
        """
        params = params or ()
        if is_sqlalchemy_conn(conn):
            qr = conn.execute(sql, *params, **kwargs)
            return qr
        qr = cursor.execute(sql, params, **kwargs)
        return cursor

    def sql_executemany(self, conn, cursor, sql, rows):
        """Bulk executes the sql statement and returns an object that can fetch results

        Parameters
        ----------
        conn
            A SQL database connection object
        cursor
            A SQL database cursor
        sql : str
            A sql query to execute
        rows
            Rows of data to bulk execute

        Returns
        -------
        cursor
            cursor object that has executed but not fetched a query.
        """
        if is_sqlalchemy_conn(conn):
            qr = conn.execute(sql, rows)
            return qr
        qr = cursor.executemany(sql, rows)
        return cursor

    def get_bulk_statement(self, conn, stmt_type, table, rows, odku=False):
        """Get a bulk execution SQL statement

        Parameters
        ----------
        conn
            A SQL database connection object
        stmt_type : str
            Type of SQL statement to use (REPLACE, INSERT, etc.)
        table : str
            name of a SQL table
        rows
            An iterable of dict rows. The first row is used to determine
            column names.
        odku : bool or list, optional
            If true, add ON DUPLICATE KEY UPDATE clause for all columns. If a
            list then only add it for the specified columns. **Note:** Backend
            support for this varies.

        Returns
        -------
        A SQL bulk replace query
        """

        """Get a bulk replace SQL statement"""
        if is_sqlalchemy_conn(conn):
            return get_bulk_statement(
                stmt_type, table, rows[0].keys(), dicts=False, odku=odku
            )

        if isinstance(conn, sqlite3.Connection):
            assert isinstance(
                rows[0], sqlite3.Row
            ), "Only sqlite3.Row rows are supported"
            return get_bulk_statement(
                stmt_type,
                table,
                rows[0].keys(),
                dicts=False,
                value_string="?",
                odku=odku,
            )

        assert not isinstance(
            rows[0], tuple
        ), "Dict rows expected, got tuple. Please use a dict cursor."
        return get_bulk_statement(stmt_type, table, rows[0].keys(), odku=odku)


class PandasSQLNode(BaseSQLNode, DataFramePushMixin):
    """Captures the connection types allowed to work with Pandas to_sql/from_sql"""

    allowed_conn_types = SQLALCHEMY_CONN_TYPES + [sqlite3.Connection]


class SQLNode(BaseSQLNode, SQLCursorPushMixin):
    """A generic SQL node that will behave differently based on the connection type"""

    allowed_conn_types = [object]

    def check_conn(self, conn):
        """Make sure the object is a valid SQL connection"""
        assert hasattr(conn, "cursor") or is_sqlalchemy_conn(
            conn
        ), "Connection must have a cursor() method or be a SQLAlchemy connection"


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

    Parameters
    ----------
    See Node documentation for parameters

    Attributes
    ----------
    executor_class
        An Executor that will be used to parallelize the push
    as_completed_func
        A callable used to get the Futures results as completed

    See Node documentation for additional attributes

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


class RuntimeContext:
    """A function to be executed at runtime to populate context values

    Parameters
    ----------
    func : callable
        The function to execute
    args
        Positional arguments to pass to func when called
    kwargs
        Keyword arguments to pass to func when called

    """

    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        return self.func(*self.args, **self.kwargs)

    def copy(self):
        return RuntimeContext(self.func, *self.args, **self.kwargs)


def get_node_contexts(pipeline):
    contexts = {k: pipeline[k].context for k in pipeline._node_lookup}
    return contexts


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


def clean_up_nodes(clean, contexts):
    """Call clean up functions for node context objects"""
    errors = []
    cleaned = set()

    # This block will clean any arg names that match regardless of node name
    removes = set()
    for node_name, context in contexts.items():
        for arg_name, arg_value in context.items():
            if arg_name in clean:
                cleaned.add((node_name, arg_name))
                removes.add(arg_name)
                func = clean[arg_name]
                try:
                    func(arg_value)
                except Exception as e:
                    dbg("Exception during clean up: %s" % str(e))

    for key in removes:
        del clean[key]

    # This block handles specific node_name/arg_name pairs
    for key, func in clean.items():
        parts = key.split("_")
        node_name = parts[0]
        arg_name = "_".join(parts[1:])

        if node_name not in contexts:
            errors.append(
                "Could not clean up %s, invalid node name: %s" % (key, node_name)
            )
            continue

        if arg_name not in contexts[node_name]:
            errors.append(
                "Could not clean up %s, invalid node arg name: %s->%s"
                % (key, node_name, arg_name)
            )
            continue

        if (node_name, arg_name) in cleaned:
            dbg("Skipping clean up for %s->%s, already cleaned" % (node_name, arg_name))
            continue

        ctx_value = contexts[node_name][arg_name]
        if not ctx_value:
            dbg("Skipping clean up for %s->%s, value is blank" % (node_name, arg_name))
            continue
        dbg("Executing clean up for %s->%s" % (node_name, arg_name))
        func(ctx_value)

    if errors:
        raise Exception("Errors during clean_up: %s" % errors)


def consume(pipeline, data, clean=None, **node_contexts):
    """Handles node contexts before/after calling pipeline.consume()

    Note
    ----
    It would have been better to subclass Pipeline and implement this logic
    right before/after the core consume() call, but there is a bug in pickle
    that prevents that from working with multiprocessing.

    """
    update_node_contexts(pipeline, node_contexts)
    try:
        contexts = get_node_contexts(pipeline)
        dbg("size=%s\n%s" % (len(data), pformat(contexts)), indent="label")
        try:
            pipeline.consume(iterize(data))
        finally:
            if clean:
                clean_up_nodes(clean, contexts)
    finally:
        reset_node_contexts(pipeline, node_contexts)


class Glider:
    """Main class for forming and executing pipelines. It thinly wraps
    Consecution's Pipeline, but does not subclass it due to a bug in pickle
    that hits an infinite recursion when using multiprocessing with a
    super().func reference.

    Parameters
    ----------
    *args
        Arguments passed through to Consecution's Pipeline class.
    **kwargs
        Keyword arguments passed through to Consecution's Pipeline class.

    Attributes
    ----------
    pipeline
        A Consecution Pipeline
    """

    def __init__(self, *args, **kwargs):
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

    @property
    def global_state(self):
        return self.pipeline.global_state

    @global_state.setter
    def global_state(self, value):
        self.pipeline.global_state = value

    def consume(self, data, clean=None, **node_contexts):
        """Setup node contexts and consume data with the pipeline

        Parameters
        ----------
        data
            Iterable of data to consume
        clean : dict, optional
            A mapping of arg names to clean up functions to be run after
            data processing is complete.
        **node_contexts
            Keyword arguments that are node_name->param_dict
        """

        """Setup node contexts and consume data with the pipeline"""
        consume(self.pipeline, data, clean=clean, **node_contexts)

    def plot(self, *args, **kwargs):
        """Passthrough to Consecution Pipeline.plot"""
        self.pipeline.plot(*args, **kwargs)

    def get_node_lookup(self):
        """Passthrough to Consecution Pipeline._node_lookup"""
        return self.pipeline._node_lookup

    def cli(self, *script_args, blacklist=None, parents=None, inject=None, clean=None):
        """Generate a decorator for this Glider that can be used to expose a CLI

        Parameters
        ----------
        *script_args
            Arguments to be added to the script CLI
        blacklist : list, optional
            List of arguments to filter from appearing in the CLI
        parents : list, optional
            List of parent CLIs to inherit from
        inject : dict, optional
            A dictionary of arg names to callables/values that inject a value
            for that arg. Those args will be passed as context to nodes that
            can accept them in their run() method.
        clean : dict, optional
            A dictionary of arg names to callables that will be used to perform
            clean up when the CLI script is complete.

        Returns
        -------
        decorator : GliderScript
            A decorator that can be used to turn a function into a CLI "main"
            function.
        """
        decorator = GliderScript(
            self,
            *script_args,
            blacklist=blacklist,
            parents=parents,
            inject=inject,
            clean=clean
        )
        return decorator


class ProcessPoolParaGlider(Glider):
    """A parallel Glider that uses a ProcessPoolExecutor to execute parallel calls to
    consume()"""

    def consume(self, data, clean=None, **node_contexts):
        """Setup node contexts and consume data with the pipeline

        Parameters
        ----------
        data
            Iterable of data to consume
        clean : dict, optional
            A mapping of arg names to clean up functions to be run after
            data processing is complete.
        **node_contexts
            Keyword arguments that are node_name->param_dict

        """
        with ProcessPoolExecutor() as executor:
            splits = np.array_split(data, min(len(data), executor._max_workers))
            futures = []
            dbg(
                "%s: %d worker(s), %d split(s)"
                % (self.__class__.__name__, executor._max_workers, len(splits))
            )
            for split in splits:
                futures.append(
                    executor.submit(
                        consume, self.pipeline, split, clean=clean, **node_contexts
                    )
                )
            for future in as_completed(futures):
                result = future.result()


class ThreadPoolParaGlider(Glider):
    """A parallel Glider that uses a ThreadPoolExecutor to execute parallel calls to
    consume()"""

    def consume(self, data, clean=None, **node_contexts):
        """Setup node contexts and consume data with the pipeline

        Parameters
        ----------
        data
            Iterable of data to consume
        clean : dict, optional
            A mapping of arg names to clean up functions to be run after
            data processing is complete.
        **node_contexts
            Keyword arguments that are node_name->param_dict

        """
        with ThreadPoolExecutor() as executor:
            splits = np.array_split(data, min(len(data), executor._max_workers))
            futures = []
            dbg(
                "%s: %d worker(s), %d split(s)"
                % (self.__class__.__name__, executor._max_workers, len(splits))
            )
            for split in splits:
                futures.append(
                    executor.submit(
                        consume, self.pipeline, split, clean=clean, **node_contexts
                    )
                )
            for future in as_completed(futures):
                result = future.result()


class GliderScript(Script):
    """A decorator that can be used to create a CLI from a Glider pipeline

    Parameters
    ----------
    glider : Glider
        A Glider pipeline to be used to auto-generate the CLI
    *script_args
        Arguments to be added to the script CLI
    blacklist : list, optional
        List of arguments to filter from appearing in the CLI
    parents : list, optional
        List of parent CLIs to inherit from
    inject : dict, optional
        A dictionary of arg names to callables/values that inject a value for
        that arg. Those args will be passed as context to nodes that can
        accept them in their run() method.
    clean : dict, optional
        A dictionary of arg names to callables that will be used to perform
        clean up when the CLI script is complete.

    """

    def __init__(
        self,
        glider,
        *script_args,
        blacklist=None,
        parents=None,
        inject=None,
        clean=None
    ):
        """Generate the script args for the given Glider and return a decorator"""
        self.glider = glider
        self.blacklist = set(blacklist or [])

        self.parents = parents or []
        assert isinstance(self.parents, list), (
            "parents must be a *list* of climax.parents: %s" % parents
        )

        self.inject = inject or {}
        if inject:
            assert isinstance(
                self.inject, dict
            ), "inject must be a dict of argname->func mappings"
            for injected_arg in inject:
                self.blacklist.add(injected_arg)

        self.clean = clean or {}
        if clean:
            assert isinstance(
                self.clean, dict
            ), "clean must be a dict of argname->func mappings"

        script_args = self._get_script_args(script_args)
        return super().__init__(*script_args)

    def __call__(self, func, *args, **kwargs):
        func = self._node_arg_converter(func, *args, **kwargs)
        return super().__call__(func, *args, **kwargs)

    def get_injected_kwargs(self):
        """Override Script method to return populated kwargs from inject arg"""
        if not self.inject:
            return {}
        result = {}
        for key, value in self.inject.items():
            if callable(value) and not isinstance(value, RuntimeContext):
                result[key] = value()
            else:
                result[key] = value
        return result

    def clean_up(self, **kwargs):
        """Override Script method to do any required clean up"""
        if not self.clean:
            return

        errors = []
        for key, func in self.clean.items():
            try:
                if key not in kwargs:
                    errors.append("Could not clean up %s, no arg found" % key)
                    continue

                value = kwargs[key]
                if isinstance(value, RuntimeContext):
                    errors.append(
                        "Attempting to call clean_up on a RuntimeContext for key: %s"
                        % key
                    )
                    continue

                func(value)
            except:
                pass
        if errors:
            raise Exception("Errors during clean_up: %s" % errors)

    def blacklisted(self, node_name, arg_name):
        """Determine if an argument has been blacklisted from the CLI"""
        if arg_name in self.blacklist:
            return True
        if self._get_script_arg_name(node_name, arg_name) in self.blacklist:
            return True
        return False

    def _get_script_arg_name(self, node_name, arg_name):
        return "%s_%s" % (node_name, arg_name)

    def _get_script_arg(self, node, arg_name, required=False, default=None):
        """Generate a tlbx Arg"""
        if self.blacklisted(node.name, arg_name):
            return

        if arg_name in self.inject:
            required = False
            default = None
        elif arg_name in node.context:
            required = False
            default = node.context[arg_name]
        elif arg_name in self.glider.global_state:
            required = False
            default = self.glider.global_state[arg_name]

        arg_type = str
        if default:
            arg_type = type(default)

        arg_name = "--" + self._get_script_arg_name(node.name, arg_name)
        if arg_type == bool:
            action = "store_true"
            if default:
                action = "store_false"
            script_arg = Arg(
                arg_name, required=required, action=action, default=default
            )
        else:
            script_arg = Arg(
                arg_name, required=required, type=arg_type, default=default
            )
        return script_arg

    def _get_script_args(self, custom_script_args=None):
        """Generate all tlbx Args for this Glider"""
        node_lookup = self.glider.get_node_lookup()
        custom_script_args = custom_script_args or []
        script_args = OrderedDict()

        if not self.blacklisted("", SCRIPT_DATA_ARG):
            script_args[SCRIPT_DATA_ARG] = Arg(SCRIPT_DATA_ARG, nargs="+")

        for node in node_lookup.values():
            for arg_name, _ in node.run_args.items():
                script_arg = self._get_script_arg(node, arg_name, required=True)
                if not script_arg:
                    continue
                script_args[script_arg.name] = script_arg

            for kwarg_name, kwarg_default in node.run_kwargs.items():
                script_arg = self._get_script_arg(
                    node, kwarg_name, required=False, default=kwarg_default
                )
                if not script_arg:
                    continue
                script_args[script_arg.name] = script_arg

        for custom_arg in custom_script_args:
            assert not self.blacklisted("", custom_arg.name), (
                "Blacklisted arg '%s' passed as a custom arg" % custom_arg.name
            )
            script_args[custom_arg.name] = custom_arg

        return script_args.values()

    def _node_arg_converter(self, func, *args, **kwargs):
        """Wrap the wrapped function so we can convert from CLI keyword args to node
        contexts"""

        def inner(data, *args, **kwargs):
            nonlocal self
            kwargs = self._convert_kwargs(kwargs)
            return func(data, *args, **kwargs)

        return inner

    def _get_injected_node_contexts(self, kwargs):
        """Populate node contexts based on injected args"""
        node_contexts = {}
        node_lookup = self.glider.get_node_lookup()
        for node in node_lookup.values():
            for arg_name, _ in node.run_args.items():
                if arg_name in self.inject:
                    node_contexts.setdefault(node.name, {})[arg_name] = kwargs[arg_name]
            for kwarg_name, kwarg_default in node.run_kwargs.items():
                if kwarg_name in self.inject:
                    node_contexts.setdefault(node.name, {})[arg_name] = kwargs[
                        kwarg_name
                    ]
        return node_contexts

    def _convert_kwargs(self, kwargs):
        """Convert flat kwargs to node contexts and remaining kwargs"""
        nodes = self.glider.get_node_lookup()
        node_contexts = {}
        add_to_final = set()

        for key, value in kwargs.items():
            key_parts = key.split("_")
            node_name = key_parts[0]

            if node_name not in nodes:
                add_to_final.add(key)
                continue

            if key in self.inject:
                add_to_final.add(key)

            assert (
                len(key_parts) > 1
            ), "Invalid keyword arg %s, can not be a node name" % (key)
            arg_name = "_".join(key_parts[1:])
            node_contexts.setdefault(node_name, {})[arg_name] = value

        injected_node_contexts = self._get_injected_node_contexts(kwargs)
        for node_name, injected_args in injected_node_contexts.items():
            if node_name in node_contexts:
                node_contexts[node_name].update(injected_args)
            else:
                node_contexts[node_name] = injected_args

        final_kwargs = dict(node_contexts=node_contexts)
        for key in add_to_final:
            final_kwargs[key] = kwargs[key]

        return final_kwargs
