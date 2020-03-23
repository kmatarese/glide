"""http://www.celeryproject.org/

This extension assumes you have setup your own celery app and imported/added
provided tasks to your app as necessary. Any code used by your Celery workers
must be importable by those workers, and you may need to make sure your app
allows pickle for serialization
"""

try:
    from celery import Task, Celery
    from celery.result import ResultSet
except:
    Celery = None
    Task = None
    ResultSet = None
from tlbx import st, import_object

# We need to import * in this case because CeleryBuildGliderTask could
# reference arbitrary Glide objects
from glide import *
from glide.utils import dbg, divide_data, flatten, size, raiseifnot

if Task:

    class CeleryGliderTask(Task):
        """A Celery Task that takes a Glider object and calls its consume()
        method in the worker."""

        serializer = "pickle"

        def run(self, data, glider, consume_kwargs=None):
            """Consume data with the given glider in the Celery worker.

            Parameters
            ----------
            data
                data to process with the glider
            glider : Glider
                Glider pipeline to consume data with
            consume_kwargs : dict, optional
                Keyword arguments to pass to consume()

            """
            glider.consume(data, **(consume_kwargs or {}))

    class CeleryGliderTemplateTask(Task):
        """A Celery Task that takes a GliderTemplate object, then builds a
        glider from that template and calls its consume() method in the
        worker."""

        serializer = "pickle"

        def run(self, data, glider_template, consume_kwargs=None):
            """Create a Glider from the given template and consume data

            Parameters
            ----------
            data
                data to process with the glider
            glider_template : GliderTemplate
                A template for building a glider
            consume_kwargs : dict, optional
                Keyword arguments to pass to consume()

            """
            glider = glider_template()
            glider.consume(data, **(consume_kwargs or {}))

    class CeleryConsumeTask(Task):
        """A Celery Task that mimics the consume() function. In order to use a
        CeleryParaGlider you must have a CeleryConsumeTask registered on your
        Celery app that you pass to CeleryParaGlider at init time."""

        serializer = "pickle"

        def run(self, pipeline, data, cleanup=None, **node_contexts):
            """Call the consume() function as a Celery Task

            Parameters
            ----------
            pipeline
                A Consecution Pipeline that will be passed the data
            data
                The data to consume with the pipeline
            cleanup : dict, optional
                A mapping of arg names to clean up functions to be run after
                data processing is complete.
            **node_contexts
                Keyword arguments that are node_name->param_dict

            """
            consume(pipeline, data, cleanup=cleanup, **node_contexts)

    class CeleryBuildGliderTask(Task):
        """A Celery Task that uses its arguments to build a Glider and call
        consume() within the celery worker. The main purpose of this is to
        provide a method to run pipelines in workers via a broker that only
        supports JSON serialization, as many Glide objects are not JSON
        serializable. If your Celery setup allows pickle you should probably
        stick to other options as this has limitations in DAG structure"""

        def run(self, data, node_init, glider_kwargs=None, consume_kwargs=None):
            """Build a Glider from the input parameters and call consume

            Note
            ----
            Currently node_init is limited in the types of DAGs it can
            represent. Grouping nodes as sublists to form a layer is
            supported.

            Parameters
            ----------
            data
                data to process with the built glider
            node_init
                An iterable of nodes, where nodes are represented as
                JSON-serializable dicts and optionally grouped as layers with
                sublists.
            glider_kwargs : dict, optional
                Keyword arguments to pass to the constructed Glider.
            consume_kwargs : type, optional
                Keyword arguments to pass to consume()

            """
            nodes = None

            for node_init_layer in node_init:
                if not isinstance(node_init_layer, (list, tuple)):
                    node_init_layer = [node_init_layer]

                node_layer = []
                for node_info in node_init_layer:
                    # TODO: this only supports a single level of node tree evaluation
                    raiseifnot(
                        isinstance(node_info, dict), "Node info must be in dict format"
                    )
                    node_args = node_info["args"]
                    node_kwargs = node_info.get("kwargs", {})
                    cls = import_object(node_info["class_name"])
                    node = cls(*node_args, **node_kwargs)
                    node_layer.append(node)

                if len(node_layer) == 1:
                    node_layer = node_layer[0]

                if nodes is None:
                    nodes = node_layer
                    continue
                nodes = nodes | node_layer

            glider = Glider(nodes, **(glider_kwargs or {}))
            glider.consume(data, **(consume_kwargs or {}))


class CeleryParaGlider(ParaGlider):
    """A ParaGlider that uses Celery to execute parallel calls to consume()

    Parameters
    ----------
    consume_task
        A Celery Task that will behave like consume()
    *args
        Arguments passed through to ParaGlider init
    **kwargs
        Keyword arguments passed through to ParaGlider init

    Attributes
    ----------
    consume_task
        A Celery Task that behaves like consume(), such as CeleryConsumeTask.
    See ParaGlider for additional attributes.

    """

    def __init__(self, consume_task, *args, **kwargs):
        raiseifnot(Celery, "Please install Celery to use CeleryParaGlider")
        raiseifnot(
            isinstance(consume_task, Task),
            (
                "The first argument to CeleryParaGlider must be a registered "
                "celery task that mirrors consume()"
            ),
        )
        self.consume_task = consume_task
        super().__init__(*args, **kwargs)

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
            is to inspect the celery app and set split_count = worker count.
        synchronous : bool, optional
            If False, return AsyncResults. If True, wait for tasks to complete and
            return their results, if any.
        timeout : int or float, optional
            If waiting for results, pass this as timeout to AsyncResult.get().
        **node_contexts
            Keyword arguments that are node_name->param_dict

        """
        if not split_count:
            dbg("determining split count from app celery worker count")
            app_stats = self.consume_task.app.control.inspect().stats()
            split_count = len(app_stats.keys())

        split_count = split_count_helper(data, split_count)
        splits = divide_data(data, split_count)
        dbg(
            "%s: data len: %s, splits: %d"
            % (self.__class__.__name__, size(data, "n/a"), split_count)
        )

        async_results = []
        for split in splits:
            async_results.append(
                self.consume_task.delay(
                    self.pipeline, split, cleanup=cleanup, **node_contexts
                )
            )

        if synchronous:
            results = []
            for async_result in async_results:
                try:
                    results.append(async_result.get(timeout=timeout))
                finally:
                    async_result.forget()
            return results

        return async_results


class CeleryApplyAsync(Node):
    """A Node that calls apply_async on a given Celery Task"""

    def run(self, data, task, timeout=None, push_type=PushTypes.Async, **kwargs):
        """Call task.apply_async with the given data as the first task argument

        Parameters
        ----------
        data
            Data to process
        task
            A Celery Task registered with your app.
        timeout : int, optional
            A timeout to use if waiting for results via AsyncResult.get()
        push_type : str, optional
            If "async", push the AsyncResult immediately.
            If "input", push the input data immediately after task submission.
            If "result", collect the task result synchronously and push it.
        **kwargs
            Keyword arguments pass to task.apply_async

        """
        async_result = task.apply_async(args=(data,), **kwargs)

        if push_type == PushTypes.Async:
            self.push(async_result)
        elif push_type == PushTypes.Input:
            self.push(data)
        elif push_type == PushTypes.Result:
            result = async_result.get(timeout=timeout)
            async_result.forget()
            self.push(result)
        else:
            raise AssertionError("Invalid push_type: %s" % push_type)


class CelerySendTask(Node):
    """A Node that calls app.send_task"""

    def run(
        self, data, app, task_name, timeout=None, push_type=PushTypes.Async, **kwargs
    ):
        """Call app.send_task with the given data as the first task argument

        Parameters
        ----------
        data
            Data to process
        app
            Celery app
        task_name
            A name of a Celery Task registered with your app.
        timeout : int, optional
            A timeout to use if waiting for results via AsyncResult.get()
        push_type : str, optional
            If "async", push the AsyncResult immediately.
            If "input", push the input data immediately after task submission.
            If "result", collect the task result synchronously and push it.
        **kwargs
            Keyword arguments pass to task.send_task

        """
        async_result = app.send_task(task_name, args=(data,), **kwargs)

        if push_type == PushTypes.Async:
            self.push(async_result)
        elif push_type == PushTypes.Input:
            self.push(data)
        elif push_type == PushTypes.Result:
            result = async_result.get(timeout=timeout)
            async_result.forget()
            self.push(result)
        else:
            raise AssertionError("Invalid push_type: %s" % push_type)


class CeleryReduce(Reduce):
    """Collect the asynchronous results before pushing"""

    def end(self):
        """Do the push once all results are in"""
        dbg("Waiting for %d celery task(s)..." % len(self.results))
        result_set = ResultSet(self.results)
        results = result_set.get(
            timeout=self.context.get("timeout", None),
            propagate=self.context.get("propagate", True),
            interval=self.context.get("interval", 0.5),
        )
        result_set.forget()
        if results and self.context.get("flatten", False):
            results = flatten(results)
        self.push(results)
