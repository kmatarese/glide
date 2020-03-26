import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import shutil

import numpy as np
from tlbx import st, poll_call

from glide.core import Node, PushNode, NoInputNode
from glide.utils import (
    dbg,
    info,
    raiseifnot,
    is_pandas,
    divide_data,
    flatten,
    join,
    window,
    split_count_helper,
    get_date_windows,
    get_datetime_windows,
    get_file_handle,
    cancel_asyncio_tasks,
)


class SkipFalseNode(Node):
    """This overrides the behavior of calling run() such that if a "false"
    object is pushed it will never call run, just push to next node instead"""

    def _run(self, data, *args, **kwargs):
        if self._debug:
            st()

        if is_pandas(data):
            if data.empty:
                self.push(data)
                return
        elif hasattr(data, "__bool__") or hasattr(data, "__len__"):
            try:
                truth = bool(data)
            except:
                truth = data is not None
            if not truth:
                self.push(data)
                return
        else:
            raise AssertionError(
                "Could not figure out how to do bool check of object: %s" % data
            )

        self.run(data, *args, **kwargs)


class IterPush(Node):
    """Push each item of an iterable individually"""

    def run(self, data, **kwargs):
        for row in data:
            self.push(row)


class WindowPush(Node):
    def run(self, data, size, **kwargs):
        """Push windows of the specified size

        Parameters
        ----------
        data
            The data to slice into windows
        size : int
            The window size

        """
        for w in window(data, size=size):
            self.push(w)


class SplitPush(Node):
    """A node that splits the data before pushing.

    If the data is a Pandas object it will use np.array_split, otherwise it
    will split the iterator into chunks of roughly equal size.

    """

    def get_splits(self, data, split_count):
        """Split the data into split_count slices"""
        return divide_data(data, split_count)

    def run(self, data, split_count, **kwargs):
        """Split the data and push each slice"""
        splits = self.get_splits(data, split_count_helper(data, split_count))
        for split in splits:
            self.push(split)


class SplitByNode(PushNode):
    """A node that splits the data based on the number of immediate downstream
    nodes.

    If the data is a Pandas object it will use np.array_split, otherwise it
    will split the iterator into chunks of roughly equal size.

    """

    def get_splits(self, data, split_count):
        """Split the data into split_count slices"""
        return divide_data(data, split_count)

    def _push(self, data):
        """Override Consecution's push such that we can push split data"""
        splits = self.get_splits(data, len(self._downstream_nodes))
        for i, split in enumerate(splits):
            self._downstream_nodes[i]._process(split)


class ArraySplitPush(SplitPush):
    """A node that splits the data before pushing"""

    def get_splits(self, data, split_count):
        """Split the data into split_count slices"""
        return np.array_split(data, split_count)


class ArraySplitByNode(SplitByNode):
    """A node that splits the data before pushing"""

    def get_splits(self, data, split_count):
        """Split the data into split_count slices"""
        return np.array_split(data, split_count)


class Flatten(Node):
    """Flatten the input before pushing"""

    def run(self, data):
        """Flatten the input before pushing. Assumes data is in ~list of ~lists format"""
        self.push(flatten(data))


class Join(Node):
    """Join iterables before pushing"""

    def run(self, data, on=None, how="left", rsuffixes=None):
        """Join items before pushing. This converts each dataset to a
        DataFrame and reuses pandas join method under the hood.

        Parameters
        ----------
        data
            The datasets to join (i.e. a list of datasets or DataFrames)
        on : optional
            Passed to the underlying pandas join method
        how : str, optional
            Passed to the underlying pandas join method
        rsuffixes : list, optional
            A list of suffixes to append to duplicate column names in the right
            datasets. The length of this should be len(data) - 1.

        """
        self.push(join(data, on=on, how=how, rsuffixes=rsuffixes))


class FuturesPush(PushNode):
    """A node that either splits or duplicates its input to pass to multiple
    downstream nodes in parallel according to the executor_class that supports
    the futures interface. If an executor_kwargs dict is in the context of
    this node it will be passed to the parallel executor.

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

    def _push(self, data):
        """Override Consecution's push such that we can push in parallel"""
        if self._logging == "output":
            self._write_log(data)

        executor_kwargs = self.context.get("executor_kwargs", None) or {}
        with self.executor_class(**executor_kwargs) as executor:
            futures = []

            do_split = self.context.get("split", False)
            info(
                "%s: split=%s, %d downstream nodes"
                % (self.__class__.__name__, do_split, len(self._downstream_nodes)),
                label="push",
            )

            if do_split:
                # Split the data among the downstream nodes
                splits = divide_data(data, len(self._downstream_nodes))
                for i, split in enumerate(splits):
                    node = self._downstream_nodes[i]
                    futures.append(executor.submit(node._process, split))
            else:
                # Pass complete data to each downstream node
                for downstream in self._downstream_nodes:
                    futures.append(executor.submit(downstream._process, data))

            # Wait for results
            for future in self.__class__.as_completed_func(futures):
                future.result()


class ProcessPoolPush(FuturesPush):
    """A multi-process FuturesPushNode"""

    pass


class ThreadPoolPush(FuturesPush):
    """A multi-threaded FuturesPushNode"""

    executor_class = ThreadPoolExecutor


class Reduce(Node):
    """Waits until end() to call push(), effectively waiting for all nodes before
    it to finish before continuing the pipeline.

    The following are parameters that get pulled from the node context and
    used in end().

    Parameters
    ----------
    flatten : bool, optional
        Flatten the results into a single list before pushing

    """

    def begin(self):
        """Setup a place for results to be collected"""
        self.results = []

    def run(self, data, **kwargs):
        """Collect results from previous nodes"""
        self.results.append(data)

    def end(self):
        """Do the push once all results are in"""
        results = self.results
        if results and self.context.get("flatten", False):
            results = flatten(results)
        self.push(results)


class Return(Reduce):
    """Collects upstream data and sets the result in the global state

    Notes
    -----
    Because this relies on the pipeline's global_state under the hood it
    will not work with pipelines that do process branching mid-pipeline
    such as ProcessPoolPush.

    Parameters
    ----------
    flatten : bool, optional
        Flatten the results into a single list before returning

    """

    def end(self):
        """Collects upstream data and sets the result in the global state"""
        results = self.results
        if results and self.context.get("flatten", False):
            results = flatten(results)
        self.set_global_results(results)


class WindowReduce(Node):
    def begin(self):
        """Initialize a place for a window to be collected"""
        self.window = []

    def run(self, data, size, **kwargs):
        """Collect results to fill and push windows

        Parameters
        ----------
        data
            Data to collect into window
        size : int
            Size of window to collect

        """
        raiseifnot(
            size and int(size) and size > 1, "Window size must be an integer > 1"
        )

        self.window.append(data)

        if len(self.window) < size:
            return

        # Final item of window will get appended next iteration
        next_window = self.window[1:]
        self.push(self.window)
        self.window = next_window


class ThreadReduce(Reduce):
    """A plain-old Reducer with a name that makes it clear it works with threads"""

    pass


class FuturesReduce(Reduce):
    """Collect results from futures before pushing

    The following are parameters that get pulled from the node context and
    used in end().

    Parameters
    ----------
    flatten : bool, optional
        Flatten the results into a single list before pushing
    timeout : int or float, optional
        Timeout to pass to futures.as_completed()

    """

    def end(self):
        """Do the push once all Futures results are in"""
        dbg("Waiting for %d futures..." % len(self.results))
        timeout = self.context.get("timeout", None)
        results = []
        for future in as_completed(self.results, timeout=timeout):
            results.append(future.result())
        if results and self.context.get("flatten", False):
            results = flatten(results)
        self.push(results)


class AsyncIOFuturesReduce(Reduce):
    """Collect results from asyncio futures before pushing

    The following are parameters that get pulled from the node context and
    used in end().

    Parameters
    ----------
    flatten : bool, optional
        Flatten the results into a single list before pushing
    timeout : int or float, optional
        Timeout to pass to asyncio.wait
    close : bool, optional
        Whether to call loop.close() after processing is done

    """

    def end(self):
        """Do the push once all Futures results are in"""
        dbg("Waiting for %d async futures..." % len(self.results))
        timeout = self.context.get("timeout", None)
        close = self.context.get("close", None)

        loop = asyncio.get_event_loop()

        try:
            done, pending = loop.run_until_complete(
                asyncio.wait(self.results, timeout=timeout)
            )
            if timeout and pending:
                cancel_asyncio_tasks(
                    pending, loop, cancel_timeout=ASYNCIO_CANCEL_TIMEOUT
                )
                raise asyncio.TimeoutError(
                    "%d/%d tasks pending" % (len(pending), len(self.results))
                )
            results = [task.result() for task in done]
        finally:
            if close:
                loop.close()

        if results and self.context.get("flatten", False):
            results = flatten(results)
        self.push(results)


class FileCopy(Node):
    """Copy one file to another"""

    def run(self, f_in, f_out, in_flags="rb", out_flags="wb", push_input=False):
        """Copy f_in to f_out and push file reference

        Parameters
        ----------
        f_in : file path or buffer
            File path or buffer to read
        f_out : file path or buffer
            File path or buffer to write
        in_flags : str, optional
            Flags to use when opening the input file
        out_flags : str, optional
            Flags to use when opening the output file
        push_input : bool, optional
            If true, push f_in instead of f_out

        """
        in_is_text = True
        out_is_text = True
        if "b" in in_flags:
            in_is_text = False
        if "b" in out_flags:
            out_is_text = False

        with get_file_handle(
            f_in, in_flags, is_text=in_is_text
        ) as src, get_file_handle(f_out, out_flags, is_text=out_is_text) as dst:
            shutil.copyfileobj(src, dst)
        if push_input:
            self.push(f_in)
        else:
            self.push(f_out)


class FileConcat(Node):
    """Concat a set of input files into one output file"""

    def run(self, files, f_out, in_flags="rb", out_flags="wb", push_input=False):
        """Concat a set of input files into one output file

        Parameters
        ----------
        f_in : file path or buffer
            File path or buffer to read
        f_out : file path or buffer
            File path or buffer to write
        in_flags : str, optional
            Flags to use when opening the input file
        out_flags : str, optional
            Flags to use when opening the output file
        push_input : bool, optional
            If true, push f_in instead of f_out

        """
        in_is_text = True
        out_is_text = True
        if "b" in in_flags:
            in_is_text = False
        if "b" in out_flags:
            out_is_text = False

        with get_file_handle(f_out, out_flags, is_text=out_is_text) as dst:
            for f_in in files:
                with get_file_handle(f_in, in_flags, is_text=in_is_text) as src:
                    shutil.copyfileobj(src, dst)

        if push_input:
            self.push(files)
        else:
            self.push(f_out)


class PollFunc(Node):
    def run(
        self,
        data,
        func,
        result_param="status",
        result_value="success",
        sleep_time=2,
        max_iter=10,
        data_param=None,
        **kwargs
    ):
        """Poll a function for a result

        Parameters
        ----------
        data
            Data to pass to func. Typically a request or URL that needs to be
            polled for a result.
        func : callable
            The function that will be called on each iteration to get a
            result. It is expected to return a dict with a key/value
            representing completion (see result_param/result_value).
        result_param : str
            The key to extract from the func result to look for success.
        result_value
            The value representing success. Keep polling until this value is found.
        sleep_time : float
            The amount of time to sleep between iterations
        max_iter : int
            The maximum number of iterations before giving up
        data_param : str, optional
            If given, pull this param out of the func result on success and
            push. Otherwise push the full response from func.
        kwargs
            Keyword arguments passed to func

        """
        result = poll_call(
            func, result_param, result_value, sleep_time, max_iter, data, **kwargs
        )
        if data_param:
            self.push(result[data_param])
        else:
            self.push(result)


class DateTimeWindowPush(NoInputNode):
    def run(
        self,
        start_date,
        end_date,
        window_size_hours=None,
        num_windows=None,
        reverse=False,
        add_second=True,
    ):
        dt_windows = get_datetime_windows(
            start_date,
            end_date,
            window_size_hours=window_size_hours,
            num_windows=num_windows,
            reverse=reverse,
            add_second=add_second,
        )
        self.push(dt_windows)


class DateWindowPush(NoInputNode):
    def run(self, start_date, end_date, reverse=False):
        date_windows = get_date_windows(start_date, end_date, reverse=reverse)
        self.push(date_windows)
