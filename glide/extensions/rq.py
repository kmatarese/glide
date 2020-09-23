"""http://python-rq.org/docs/"""

import time

try:
    from rq import Queue, Worker
except:
    Redis = None
    Queue = None
    Worker = None
from tlbx import st

from glide import ParaGlider, Node, PushTypes, consume
from glide.flow import Reduce, split_count_helper
from glide.utils import dbg, divide_data, flatten, size, raiseifnot


POLL_SLEEP = 1


class RQTimeoutException(Exception):
    """Exception for timeouts polling for results"""

    pass


def complete_count(async_results):
    """
    TODO: Would it be better to rely on the job registry instead of job.result?

    --- Example:
    from rq import job
    from rq.registry import FinishedJobRegistry
    registry = FinishedJobRegistry('default', connection=redis_conn)
    job_ids = registry.get_job_ids()
    job_obj = job.Job.fetch("job-id-here", connection=redis_conn)

    """
    count = 0
    for job in async_results:
        if job.result is not None:
            count += 1
    return count


def get_async_results(async_results, timeout=None):
    """Poll for results """
    # TODO: Is there a better option than polling?
    start = time.time()

    while complete_count(async_results) < len(async_results):
        diff = time.time() - start
        if timeout and diff >= timeout:
            raise RQTimeoutException("get_async_results timed out after %.3fs" % diff)

        dbg("Sleeping %.3fs..." % POLL_SLEEP)
        time.sleep(POLL_SLEEP)

    return [job.result for job in async_results]


def get_async_result(async_result, timeout=None):
    """Poll for a result"""
    results = get_async_results([async_result], timeout=timeout)
    return results[0]


def rq_consume(*args, **kwargs):
    """Hack: RQ only seems to update the job status if your function returns
    a non-None value. To force that, we use this simple wrapper around consume().
    """
    return consume(*args, **kwargs) or True


class RQParaGlider(ParaGlider):
    """A ParaGlider that uses Redis Queue to execute parallel calls to consume()

    Parameters
    ----------
    queue
        An rq Queue object
    *args
        Arguments passed through to ParaGlider init
    **kwargs
        Keyword arguments passed through to ParaGlider init

    Attributes
    ----------
    queue
        An rq Queue object
    See ParaGlider for additional attributes.

    """

    def __init__(self, queue, *args, **kwargs):
        raiseifnot(Queue, "Please install 'rq' to use RQParaGlider")
        raiseifnot(
            isinstance(queue, Queue),
            "The first argument to RQParaGlider must be a Queue",
        )
        self.queue = queue
        super().__init__(*args, **kwargs)

    def consume(
        self,
        data=None,
        cleanup=None,
        split_count=None,
        synchronous=False,
        timeout=None,
        **node_contexts
    ):
        """Setup node contexts and consume data with the pipeline

        Parameters
        ----------
        data : iterable, optional
            Iterable of data to consume
        cleanup : dict, optional
            A mapping of arg names to clean up functions to be run after
            data processing is complete.
        split_count : int, optional
            How many slices to split the data into for parallel processing. Default
            is the number of workers in the provided queue.
        synchronous : bool, optional
            If False, return Jobs. If True, wait for jobs to complete and
            return their results, if any.
        timeout : int or float, optional
            If waiting for results, raise an exception if polling for all
            results takes longer than timeout seconds.
        **node_contexts
            Keyword arguments that are node_name->param_dict

        """
        if not split_count:
            dbg("determining split count from rq worker count")
            workers = Worker.all(queue=self.queue)
            split_count = len(workers)

        split_count = split_count_helper(data, split_count)
        if data is None:
            splits = [None for s in range(split_count)]
        else:
            splits = divide_data(data, split_count)

        dbg(
            "%s: data len: %s, splits: %d"
            % (self.__class__.__name__, size(data, "n/a"), split_count)
        )

        async_results = []
        for split in splits:
            async_results.append(
                self.queue.enqueue(
                    rq_consume,
                    args=(self.pipeline, split),
                    kwargs=dict(cleanup=cleanup, **node_contexts),
                )
            )

        if synchronous:
            return get_async_results(async_results, timeout=timeout)

        return async_results


class RQJob(Node):
    """A Node that queues a function using Redis Queue

    Warnings
    --------
    Python RQ seems to not update the job status if your function does not
    return a non-None value. Your code may hang if you poll waiting for a
    result in this scenario.
    """

    def run(
        self,
        data,
        func,
        queue=None,
        queue_name="default",
        redis_conn=None,
        push_type=PushTypes.Async,
        poll_sleep=POLL_SLEEP,
        timeout=None,
        **kwargs
    ):
        """Execute func on data using Redis Queue

        Parameters
        ----------
        data
            Data to process
        func : callable
            Function to execute using Redis Queue
        queue : Queue, optional
            An rq Queue object
        queue_name : str, optional
            When creating a queue, the name of the queue to use
        redis_conn : type, optional
            When creating a queue, the redis connection to use
        push_type : type, optional
            If "async", push the Job immediately.
            If "input", push the input data immediately after task submission.
            If "result", collect the task result synchronously and push it.
        poll_sleep : int or float, optional
            If waiting for the result, sleep this many seconds between polls
        timeout : int or float, optional
            If waiting for result, raise an exception if polling for all
            results takes longer than timeout seconds.
        **kwargs
            Keyword arguments to pass to enqueue()

        """
        raiseifnot(
            queue or redis_conn, "One of 'queue' or 'redis_conn' must be specified"
        )
        if not queue:
            queue = Queue(queue_name, connection=redis_conn)

        job = queue.enqueue(func, args=(data,), **kwargs)

        if push_type == PushTypes.Async:
            self.push(job)
        elif push_type == PushTypes.Input:
            self.push(data)
        elif push_type == PushTypes.Result:
            self.push(get_async_result(job, timeout=timeout))
        else:
            raise AssertionError("Invalid push_type: %s" % push_type)


class RQReduce(Reduce):
    """Collect asynchronous results before pushing"""

    def end(self):
        """Do the push once all results are in"""
        dbg("Waiting for %d RQ job(s)..." % len(self.results))
        results = get_async_results(self.results)
        if results and self.context.get("flatten", False):
            results = flatten(results)
        self.push(results)
