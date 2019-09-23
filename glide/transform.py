"""A home for common transform nodes"""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import numpy as np
import pandas as pd
from tlbx import st, json, set_missing_key, update_email

from glide.core import Node
from glide.utils import find_class_in_dict, get_class_list_docstring


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

    def run(self, df, func, **kwargs):
        """Split the DataFrame and call func() in parallel processes, concat results

        Parameters
        ----------
        df : pandas.DataFrame
            The pandas DataFrame to split and apply func to in parallel
        func : callable
            A callable that will be passed a split of the df to operate on in
            a parallel process
        **kwargs
            Keyword arguments passed to executor.map

        """
        with ProcessPoolExecutor() as executor:
            df_split = np.array_split(df, executor._max_workers)
            df = pd.concat(executor.map(func, df_split, **kwargs))
        self.push(df)


class DataFrameThreadPoolTransformer(Node):
    """Apply a transform to a Pandas DataFrame using parallel threads"""

    def run(self, df, func, **kwargs):
        """Split the DataFrame and call func() in parallel threads, concat results

        Parameters
        ----------
        df : pandas.DataFrame
            The pandas DataFrame to split and apply func to in parallel
        func : callable
            A callable that will be passed a split of the df to operate on in
            a parallel threads
        **kwargs
            Keyword arguments passed to executor.map

        """
        with ThreadPoolExecutor() as executor:
            df_split = np.array_split(df, executor._max_workers)
            df = pd.concat(executor.map(func, df_split, **kwargs))
        self.push(df)


# -------- Row-based Transformers


class RowProcessPoolTransformer(Node):
    """Apply a transform function to a set of rows in parallel processes"""

    def run(self, rows, func, **kwargs):
        """Use a ProcessPoolExecutor to map() func over input rows

        Parameters
        ----------
        rows
            An iteratable of rows to process
        func : callable
            A callable that will be passed rows to operate on in parallel
            processes
        **kwargs
            Keyword arguments passed to executor.map

        """
        result_rows = []
        with ProcessPoolExecutor() as executor:
            set_missing_key(
                kwargs, "chunksize", int(max(len(rows) / executor._max_workers, 1))
            )
            rows = [x for x in executor.map(func, rows, **kwargs)]
        self.push(rows)


class RowThreadPoolTransformer(Node):
    """Apply a transform function to a set of rows in parallel threads"""

    def run(self, rows, func, **kwargs):
        """Use a ThreadPoolExecutor to map() func over input rows

        Parameters
        ----------
        rows
            An iteratable of rows to process
        func : callable
            A callable that will be passed rows to operate on in parallel
            threads
        **kwargs
            Keyword arguments passed to executor.map

        """
        result_rows = []
        with ThreadPoolExecutor() as executor:
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
