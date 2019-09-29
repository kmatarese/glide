"""A home for common transform nodes"""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import numpy as np
import pandas as pd
from tlbx import st, json, set_missing_key, update_email

from glide.core import Node
from glide.utils import info, find_class_in_dict, get_class_list_docstring


class DummyTransformer(Node):
    """Passthrough transformer"""

    def run(self, item):
        """Just pushes the item through"""
        self.push(item)


# -------- Pandas Transformers


class DataFrameApplyMapTransformer(Node):
    """Apply a transform to a Pandas DataFrame"""

    def run(self, df, func, **kwargs):
        """Use applymap() on a DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            The pandas DataFrame to apply func to
        func : callable
            A callable that will be passed to df.applymap
        **kwargs
            Keyword arguments passed to applymap

        """
        df = df.applymap(func, **kwargs)
        self.push(df)


class DataFrameProcessPoolTransformer(Node):
    """Apply a transform to a Pandas DataFrame using parallel processes"""

    def run(self, df, func, executor_kwargs=None, **kwargs):
        """Split the DataFrame and call func() in parallel processes, concat results

        Parameters
        ----------
        df : pandas.DataFrame
            The pandas DataFrame to split and apply func to in parallel
        func : callable
            A callable that will be passed a split of the df to operate on in
            a parallel process
        executor_kwargs : dict, optional
            Keyword arguments to pass to ProcessPoolExecutor
        **kwargs
            Keyword arguments passed to executor.map

        """
        executor_kwargs = executor_kwargs or {}
        with ProcessPoolExecutor(**executor_kwargs) as executor:
            df_splits = np.array_split(df, executor._max_workers)
            info(
                "%s: data len: %s, %d worker(s), %d split(s)"
                % (
                    self.__class__.__name__,
                    len(df),
                    executor._max_workers,
                    len(df_splits),
                )
            )
            df = pd.concat(executor.map(func, df_splits, **kwargs))
        self.push(df)


class DataFrameThreadPoolTransformer(Node):
    """Apply a transform to a Pandas DataFrame using parallel threads"""

    def run(self, df, func, executor_kwargs=None, **kwargs):
        """Split the DataFrame and call func() in parallel threads, concat results

        Parameters
        ----------
        df : pandas.DataFrame
            The pandas DataFrame to split and apply func to in parallel
        func : callable
            A callable that will be passed a split of the df to operate on in
            a parallel threads
        executor_kwargs : dict, optional
            Keyword arguments to pass to ThreadPoolExecutor
        **kwargs
            Keyword arguments passed to executor.map

        """
        executor_kwargs = executor_kwargs or {}
        with ThreadPoolExecutor(**executor_kwargs) as executor:
            df_splits = np.array_split(df, executor._max_workers)
            info(
                "%s: data len: %s, %d worker(s), %d split(s)"
                % (
                    self.__class__.__name__,
                    len(df),
                    executor._max_workers,
                    len(df_splits),
                )
            )
            df = pd.concat(executor.map(func, df_splits, **kwargs))
        self.push(df)


# -------- Row-based Transformers


class RowProcessPoolTransformer(Node):
    """Apply a transform function to a set of rows in parallel processes"""

    def run(self, rows, func, executor_kwargs=None, **kwargs):
        """Use a ProcessPoolExecutor to map() func over input rows

        Parameters
        ----------
        rows
            An iteratable of rows to process
        func : callable
            A callable that will be passed rows to operate on in parallel
            processes
        executor_kwargs : dict, optional
            Keyword arguments to pass to ProcessPoolExecutor
        **kwargs
            Keyword arguments passed to executor.map

        """
        result_rows = []
        executor_kwargs = executor_kwargs or {}
        with ProcessPoolExecutor(**executor_kwargs) as executor:
            set_missing_key(
                kwargs, "chunksize", int(max(len(rows) / executor._max_workers, 1))
            )
            info(
                "%s: data len: %s, %d worker(s)"
                % (self.__class__.__name__, len(rows), executor._max_workers)
            )
            rows = [x for x in executor.map(func, rows, **kwargs)]
        self.push(rows)


class RowThreadPoolTransformer(Node):
    """Apply a transform function to a set of rows in parallel threads"""

    def run(self, rows, func, executor_kwargs=None, **kwargs):
        """Use a ThreadPoolExecutor to map() func over input rows

        Parameters
        ----------
        rows
            An iteratable of rows to process
        func : callable
            A callable that will be passed rows to operate on in parallel
            threads
        executor_kwargs : dict, optional
            Keyword arguments to pass to ThreadPoolExecutor
        **kwargs
            Keyword arguments passed to executor.map

        """
        result_rows = []
        executor_kwargs = executor_kwargs or {}
        with ThreadPoolExecutor(**executor_kwargs) as executor:
            info(
                "%s: data len: %s, %d worker(s)"
                % (self.__class__.__name__, len(rows), executor._max_workers)
            )
            rows = [x for x in executor.map(func, rows, **kwargs)]
        self.push(rows)


# -------- Other Transformers


class JSONDumpsTransformer(Node):
    def run(self, data):
        self.push(json.dumps(data))


class JSONLoadsTransformer(Node):
    def run(self, data):
        self.push(json.loads(data))


class EmailMessageTransformer(Node):
    """Update EmailMessage objects"""

    def run(
        self,
        msg,
        frm=None,
        to=None,
        subject=None,
        body=None,
        html=None,
        attachments=None,
    ):
        """Update the EmailMessage with the given arguments

        Parameters
        ----------
        msg : EmailMessage
            EmailMessage object to update
        frm : str, optional
            Update from address
        to : str, optional
            Update to address(es)
        subject : str, optional
            Update email subject
        body : str, optional
            Update email body
        html : str, optional
            Update email html
        attachments : list, optional
            Replace the email attachments with these

        """
        update_email(
            msg,
            frm=frm,
            to=to,
            subject=subject,
            body=body,
            html=html,
            attachments=attachments,
        )
        self.push(msg)


node_names = find_class_in_dict(Node, locals(), "Transform")
if node_names:
    __doc__ = __doc__ + get_class_list_docstring("Nodes", node_names)
