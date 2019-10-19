"""Core classes used to power pipelines"""

from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import copy
from inspect import signature, Parameter
import os
from pprint import pformat

from consecution import (
    Pipeline,
    GlobalState as ConsecutionGlobalState,
    Node as ConsecutionNode,
)
from numpydoc.docscrape import FunctionDoc
from tlbx import (
    st,
    repr,
    Script,
    Arg,
    Parent,
    MappingMixin,
    set_missing_key,
    format_msg,
)

from glide.utils import (
    dbg,
    info,
    is_function,
    closer,
    iterize,
    size,
    divide_data,
    flatten,
    split_count_helper,
    load_json_config,
    load_ini_config,
    load_yaml_config,
)

SCRIPT_DATA_ARG = "data"
RESERVED_NODE_NAMES = set(["cleanup", "split_count", "synchronous", "timeout"])


class PushTypes:
    """The names of push strategies for nodes that support asynchronous execution"""

    Async = "async"
    Input = "input"
    Result = "result"


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
            dbg("size:%s %s" % (size(item), repr(item)), label=self.name)
        self._run(item, *arg_values, **kwarg_values)

    def _run(self, item, *args, **kwargs):
        self.run(item, *args, **kwargs)

    def run(self, item, **kwargs):
        """Subclasses will override this method to implement core node logic"""
        raise NotImplementedError


class GroupByNode(Node):
    """This approach was copied from Consecution. It batches items
    by key and then pushes once the key changes. For that reason
    it requires sorting ahead of time to function properly. It may
    make sense to provide different behavior."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._batch_ = []
        self._previous_key = "__no_previous_key__"

    def key(self, item):
        raise NotImplementedError(
            "you must define a .key(self, item) method on all " "GroupBy nodes."
        )

    def process(self, item):
        key = self.key(item)
        if key != self._previous_key:
            self._previous_key = key
            if len(self._batch_) > 0:
                self._run(self._batch_)
            self._batch_ = [item]
        else:
            self._batch_.append(item)

    def _end(self):
        self._run(self._batch_)
        self._batch_ = []

    def __getattribute__(self, name):
        """This should trap for the end() method calls and install pre hook"""
        if name == "end":

            def wrapper():
                self._end()
                return super(GroupByNode, self).__getattribute__(name)()

            return wrapper
        else:
            return super(GroupByNode, self).__getattribute__(name)


class PushNode(Node):
    """A node that just passes all items through in run()"""

    def run(self, item, **kwargs):
        self.push(item)


class PlaceholderNode(PushNode):
    """Used as a placeholder in pipelines. Will pass values through by default"""

    pass


class PoolSubmit(Node):
    """Apply a function to the data in parallel"""

    def check_data(self, data):
        """Optional input data check"""
        return

    def get_executor(self, **executor_kwargs):
        """Override this to return the parallel executor"""
        raise NotImplementedError

    def get_worker_count(self, executor):
        """Override this to return a count of workers active in the executor"""
        raise NotImplementedError

    def submit(self, executor, func, splits, **kwargs):
        """Override this to submit work to the executor"""
        raise NotImplementedError

    def get_results(self, futures, timeout=None):
        """Override this to fetch results from an asynchronous task"""
        raise NotImplementedError

    def shutdown_executor(self, executor):
        """Override this to shutdown the executor"""
        raise NotImplementedError

    def run(
        self,
        data,
        func,
        executor=None,
        executor_kwargs=None,
        split_count=None,
        timeout=None,
        push_type=PushTypes.Async,
        **kwargs
    ):
        """Use a parallel executor to apply func to data

        Parameters
        ----------
        data
            An iterable to process
        func : callable
            A callable that will be passed data to operate on in parallel
        executor : Executor, optional
            If passed use this executor instead of creating one.
        executor_kwargs : dict, optional
            Keyword arguments to pass when initalizing an executor.
        split_count : int, optional
            How many slices to split the data into for parallel processing. Default
            is to set split_count = number of workers
        timeout : int or float, optional
            Time to wait for jobs to complete before raising an error. Ignored
            unless using a push_type that waits for results.
        push_type : type, optional
            If "async", push the Futures immediately.
            If "input", push the input data immediately after task submission.
            If "result", collect the result synchronously and push it.
        **kwargs
            Keyword arguments passed to the executor when submitting work

        """
        self.check_data(data)

        shutdown = True
        if executor:
            shutdown = False
        else:
            executor_kwargs = executor_kwargs or {}
            executor = self.get_executor(**executor_kwargs)

        try:
            worker_count = self.get_worker_count(executor)
            split_count = split_count or worker_count
            splits = divide_data(data, split_count)
            info(
                "%s: data len: %s, splits: %s, workers: %d"
                % (
                    self.__class__.__name__,
                    size(data, "n/a"),
                    split_count,
                    worker_count,
                )
            )
            futures = self.submit(executor, func, splits, **kwargs)

            if push_type == PushTypes.Async:
                for future in futures:
                    self.push(future)
            elif push_type == PushTypes.Input:
                self.push(data)
            elif push_type == PushTypes.Result:
                self.push(self.get_results(futures, timeout=timeout))
            else:
                assert False, "Invalid push_type: %s" % push_type

        finally:
            if shutdown:
                self.shutdown_executor(executor)


class ProcessPoolSubmit(PoolSubmit):
    """A PoolExecutor that uses ProcessPoolExecutor"""

    def get_executor(self, **executor_kwargs):
        return ProcessPoolExecutor(**executor_kwargs)

    def get_worker_count(self, executor):
        return executor._max_workers

    def submit(self, executor, func, splits, **kwargs):
        futures = []
        for split in splits:
            futures.append(executor.submit(func, split, **kwargs))
        return futures

    def get_results(self, futures, timeout=None):
        results = []
        for future in as_completed(futures, timeout=timeout):
            results.append(future.result())
        return flatten(results)

    def shutdown_executor(self, executor, **kwargs):
        executor.shutdown(**kwargs)


class ThreadPoolSubmit(ProcessPoolSubmit):
    """A PoolExecutor that uses ThreadPoolExecutor"""

    def get_executor(self, **executor_kwargs):
        return ThreadPoolExecutor(**executor_kwargs)


class AssertFunc(Node):
    def run(self, data, func):
        """Assert that a function returns a truthy value

        Parameters
        ----------
        data
            Data to push if func(self, data) is truthy
        func : callable
            A callable that accepts (node, data) args and returns a truthy value if
            the assertion should pass.

        """
        assert func(self, data), "Data assertion failed\nnode: %s\nfunc: %s" % (
            self.name,
            func,
        )
        self.push(data)


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
        """Create a copy of this RuntimeContext referencing the same objects"""
        return RuntimeContext(self.func, *self.args, **self.kwargs)


class ConfigContext(RuntimeContext):
    def __init__(self, filename=None, var=None, key=None):
        """Populate context values at runtime from a config file. One of
        filename or var must be specified.

        Parameters
        ----------
        filename : str, optional
            Name of a file to read the config from. The config parser used
            will be inferred from the file extension.
        var : str, optional
            The name of an environment variable that points to a config file
            to read.
        key : str or callable, optional
            A key to extract from the config, or a callable that takes the
            config and returns an extracted value

        """
        assert filename or var, "Either filename or var must be specified"
        assert not (filename and var), "Only one of filename or var should be specified"

        if var:
            filename = os.environ[var]

        ext = filename.split(".")[-1]
        supported = ["json", "yaml", "ini"]
        assert ext in supported, "Invalid extension, only %s supported" % supported

        if ext == "json":
            func = load_json_config
        elif ext == "yaml":
            func = load_yaml_config
        elif ext == "ini":
            func = load_ini_config

        super().__init__(func, filename, key=key)


def get_node_contexts(pipeline):
    """Get a dict of node_name->node_context from pipeline"""
    contexts = {k: getattr(pipeline[k], "context", {}) for k in pipeline._node_lookup}
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


def clean_up_nodes(cleanup, contexts):
    """Call clean up functions for node context objects"""
    errors = []
    cleaned = set()

    # This block will clean any arg names that match regardless of node name
    removes = set()
    for node_name, context in contexts.items():
        for arg_name, arg_value in context.items():
            if arg_name in cleanup:
                cleaned.add((node_name, arg_name))
                removes.add(arg_name)
                func = cleanup[arg_name]
                try:
                    func(arg_value)
                except Exception as e:
                    dbg("Exception during clean up: %s" % str(e))

    for key in removes:
        del cleanup[key]

    # This block handles specific node_name/arg_name pairs
    for key, func in cleanup.items():
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
        try:
            func(ctx_value)
        except Exception as e:
            errors.append(
                "Failed to clean up %s->%s: %s" % (node_name, arg_name, str(e))
            )

    if errors:
        raise Exception("Errors during clean_up: %s" % errors)


def consume(pipeline, data, cleanup=None, **node_contexts):
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
        dbg("size=%s\n%s" % (size(data, "n/a"), pformat(contexts)), indent="label")
        try:
            return pipeline.consume(iterize(data))
        finally:
            if cleanup:
                clean_up_nodes(cleanup, contexts)
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
        node_lookup = self.get_node_lookup()
        for key in node_lookup:
            assert key not in RESERVED_NODE_NAMES, (
                "Can not use reserved node name: %s" % key
            )

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
        """Get the pipeline global_state attribute"""
        return self.pipeline.global_state

    @global_state.setter
    def global_state(self, value):
        """Set the pipeline global_state attribute"""
        self.pipeline.global_state = value

    def consume(self, data, cleanup=None, **node_contexts):
        """Setup node contexts and consume data with the pipeline

        Parameters
        ----------
        data
            Iterable of data to consume
        cleanup : dict, optional
            A mapping of arg names to clean up functions to be run after
            data processing is complete.
        **node_contexts
            Keyword arguments that are node_name->param_dict
        """
        consume(self.pipeline, data, cleanup=cleanup, **node_contexts)

    def plot(self, *args, **kwargs):
        """Passthrough to Consecution Pipeline.plot"""
        self.pipeline.plot(*args, **kwargs)

    def get_node_lookup(self):
        """Passthrough to Consecution Pipeline._node_lookup"""
        return self.pipeline._node_lookup

    def cli(
        self, *script_args, blacklist=None, parents=None, inject=None, cleanup=None
    ):
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
            A dictionary of arg names to functions/values that inject a value
            for that arg. Those args will be passed as context to nodes that
            can accept them in their run() method.
        cleanup : dict, optional
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
            cleanup=cleanup
        )
        return decorator


class ParaGlider(Glider):
    """

    Parameters
    ----------
    *args
        Arguments passed through to Glider
    executor_kwargs : dict, optional
        A dict of keyword arguments to pass to the process or thread executor
    **kwargs
        Keyword arguments passed through to Glider

    Attributes
    ----------
    pipeline
        A Consecution Pipeline
    executor_kwargs
        A dict of keyword arguments to pass to the process or thread executor

    """

    def __init__(self, *args, executor_kwargs=None, **kwargs):
        self.executor_kwargs = executor_kwargs or {}
        super().__init__(*args, **kwargs)

    def get_executor(self):
        """Override this method to create the parallel executor"""
        raise NotImplementedError

    def get_worker_count(self, executor):
        """Override this method to get the active worker count from the executor"""
        raise NotImplementedError

    def get_results(self, futures, timeout=None):
        """Override this method to get the asynchronous results"""
        raise NotImplementedError

    def consume(
        self,
        data,
        cleanup=None,
        split_count=None,
        synchronous=False,
        timeout=None,
        **node_contexts
    ):
        """Setup node contexts and consume data with the pipeline

        Parameters
        ----------
        data
            Iterable of data to consume
        cleanup : dict, optional
            A mapping of arg names to clean up functions to be run after
            data processing is complete.
        split_count : int, optional
            How many slices to split the data into for parallel processing. Default
            is to use executor._max_workers.
        synchronous : bool, optional
            If False, return Futures. If True, wait for futures to complete and
            return their results, if any.
        timeout : int or float, optional
            Raises a concurrent.futures.TimeoutError if __next__() is called
            and the result isn’t available after timeout seconds from the
            original call to as_completed(). Ignored if synchronous=False.
        **node_contexts
            Keyword arguments that are node_name->param_dict

        """
        with self.get_executor() as executor:
            worker_count = self.get_worker_count(executor)
            split_count = split_count_helper(data, split_count or worker_count)
            splits = divide_data(data, split_count)
            futures = []

            info(
                "%s: data len: %s, splits: %d, workers: %d"
                % (
                    self.__class__.__name__,
                    size(data, "n/a"),
                    worker_count,
                    split_count,
                )
            )

            for split in splits:
                futures.append(
                    executor.submit(
                        consume, self.pipeline, split, cleanup=cleanup, **node_contexts
                    )
                )

            if synchronous:
                return self.get_results(futures, timeout=timeout)

            return futures


class ProcessPoolParaGlider(ParaGlider):
    """A parallel Glider that uses a ProcessPoolExecutor to execute parallel calls to
    consume()"""

    def get_executor(self):
        return ProcessPoolExecutor(**self.executor_kwargs)

    def get_worker_count(self, executor):
        return executor._max_workers

    def get_results(self, futures, timeout=None):
        results = []
        for future in as_completed(futures, timeout=timeout):
            results.append(future.result())
        return results


class ThreadPoolParaGlider(ProcessPoolParaGlider):
    """A parallel Glider that uses a ThreadPoolExecutor to execute parallel calls to
    consume()"""

    def get_executor(self):
        return ThreadPoolExecutor(**self.executor_kwargs)


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
        A dictionary of arg names to functions/values that inject a value for
        that arg. Those args will be passed as context to nodes that can
        accept them in their run() method.
    cleanup : dict, optional
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
        cleanup=None
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

        self.cleanup = cleanup or {}
        if self.cleanup:
            assert isinstance(
                self.cleanup, dict
            ), "cleanup must be a dict of argname->func mappings"

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
            if is_function(value) and not isinstance(value, RuntimeContext):
                result[key] = value()
            else:
                result[key] = value
        return result

    def clean_up(self, **kwargs):
        """Override Script method to do any required clean up"""
        if not self.cleanup:
            return

        errors = []
        for key, func in self.cleanup.items():
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

    def _get_script_arg(
        self, node, arg_name, required=False, default=None, arg_help=None
    ):
        """Generate a tlbx Arg"""
        if self.blacklisted(node.name, arg_name):
            return None

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
        if default is not None:
            arg_type = type(default)

        arg_name = "--" + self._get_script_arg_name(node.name, arg_name)
        if arg_type == bool:
            action = "store_true"
            if default:
                action = "store_false"
            script_arg = Arg(
                arg_name,
                required=required,
                action=action,
                default=default,
                help=arg_help,
            )
        else:
            script_arg = Arg(
                arg_name,
                required=required,
                type=arg_type,
                default=default,
                help=arg_help,
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
            try:
                # Only works if run() has docs in numpydoc format
                docs = FunctionDoc(node.run)
                node_help = {v.name: "\n".join(v.desc) for v in docs["Parameters"]}
            except Exception as e:
                info("failed to parse node '%s' run() docs: %s" % (node.name, str(e)))
                node_help = {}

            for arg_name, _ in node.run_args.items():
                arg_help = node_help.get(arg_name, None)
                script_arg = self._get_script_arg(
                    node, arg_name, required=True, arg_help=arg_help
                )
                if not script_arg:
                    continue
                script_args[script_arg.name] = script_arg

            for kwarg_name, kwarg_default in node.run_kwargs.items():
                arg_help = node_help.get(kwarg_name, None)
                script_arg = self._get_script_arg(
                    node,
                    kwarg_name,
                    required=False,
                    default=kwarg_default,
                    arg_help=arg_help,
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

            for kwarg_name, _ in node.run_kwargs.items():
                if kwarg_name in self.inject:
                    node_contexts.setdefault(node.name, {})[kwarg_name] = kwargs[
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
