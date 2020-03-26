"""https://pandas.pydata.org/"""

try:
    import pandas as pd
except ImportError:
    pd = None
import sqlite3
from tlbx import st

from glide.core import Node
from glide.sql import BaseSQLNode
from glide.sql_utils import build_table_select, get_temp_table, SQLALCHEMY_CONN_TYPES
from glide.utils import warn, listify, raiseif, raiseifnot


class ToDataFrame(Node):
    def run(self, rows, **kwargs):
        """Convert the rows to a DataFrame

        Parameters
        ----------
        rows
            An iterable of rows to convert to a DataFrame
        **kwargs
            Keyword arguments passed to from_records()

        """
        df = pd.DataFrame.from_records(rows, **kwargs)
        self.push(df)


class FromDataFrame(Node):
    def run(self, df, orient="records", **kwargs):
        """Push the DataFrame to the next node, obeying chunksize if passed

        Parameters
        ----------
        df
            A DataFrame to convert to an iterable of records
        orient
            The orient arg passed to df.to_dict()
        **kwargs
            Keyword arguments passed to df.to_dict()

        """
        rows = df.to_dict(orient=orient, **kwargs)
        self.push(rows)


class DataFrameMethod(Node):
    """Helper to execute any pandas DataFrame method"""

    def run(self, df, method, **kwargs):
        """Helper to execute any pandas DataFrame method

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame object used to run the method
        method : str
            A name of a valid DataFrame method
        **kwargs
            Arguments to pass to the DataFrame method

        """
        m = getattr(df, method, None)
        raiseifnot(m and callable(m), "Invalid DataFrame method: %s" % m)
        self.push(m(**kwargs))


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


class DataFramePush(Node, DataFramePushMixin):
    """Base class for DataFrame-based nodes"""

    pass


class PandasSQLNode(BaseSQLNode, DataFramePushMixin):
    """Captures the connection types allowed to work with Pandas to_sql/from_sql"""

    allowed_conn_types = SQLALCHEMY_CONN_TYPES + [sqlite3.Connection]


class DataFrameCSVExtract(DataFramePush):
    """Extract data from a CSV using Pandas"""

    def run(self, f, **kwargs):
        """Extract data for input file and push as a DataFrame

        Parameters
        ----------
        f
            file or buffer to be passed to pandas.read_csv
        **kwargs
            kwargs to be passed to pandas.read_csv

        """
        raiseifnot(pd, "Please install Pandas to use this class")
        df = pd.read_csv(f, **kwargs)
        self.do_push(df, chunksize=kwargs.get("chunksize", None))


class DataFrameExcelExtract(DataFramePush):
    """Extract data from an Excel file using Pandas"""

    def run(self, f, **kwargs):
        """Extract data for input file and push as a DataFrame. This will push a
        DataFrame or dict of DataFrames in the case of reading multiple sheets
        from an Excel file.

        Parameters
        ----------
        f
            file or buffer to be passed to pandas.read_excel
        **kwargs
            kwargs to be passed to pandas.read_excel

        """
        raiseifnot(pd, "Please install Pandas to use this class")
        df_or_dict = pd.read_excel(f, **kwargs)
        self.do_push(df_or_dict)


class DataFrameSQLExtract(PandasSQLNode):
    """Extract data from a SQL db using Pandas"""

    def run(self, sql, conn, **kwargs):
        """Extract data for input query and push as a DataFrame

        Parameters
        ----------
        sql
            SQL query to pass to pandas.read_sql
        conn
            A SQL database connection
        **kwargs
            kwargs to be passed to pandas.read_sql

        """
        raiseifnot(pd, "Please install Pandas to use this class")
        df = pd.read_sql(sql, conn, **kwargs)
        self.do_push(df, kwargs.get("chunksize", None))


class DataFrameSQLTableExtract(PandasSQLNode):
    """Extract data from a SQL table using Pandas"""

    def run(self, table, conn, where=None, limit=None, **kwargs):
        """Extract data for input table and push as a DataFrame

        Parameters
        ----------
        table : str
            SQL table to query
        conn
            A SQL database connection
        where : str, optional
            A SQL where clause
        limit : int, optional
            Limit to put in SQL limit clause
        **kwargs
            kwargs to be passed to pandas.read_sql

        """
        raiseifnot(pd, "Please install Pandas to use this class")
        sql = build_table_select(table, where=where, limit=limit)
        df = pd.read_sql(sql, conn, **kwargs)
        self.do_push(df, kwargs.get("chunksize", None))


class DataFrameHTMLExtract(Node):
    """Extract data from HTML tables using Pandas"""

    def run(self, f, **kwargs):
        """Extract data for input file and push as a DataFrame

        Parameters
        ----------
        f
            file or buffer to be passed to pandas.read_html
        **kwargs
            kwargs to be passed to pandas.read_html

        """
        raiseifnot(pd, "Please install Pandas to use this class")
        df_list = pd.read_html(f, **kwargs)
        self.push(df_list)


class DataFrameCSVLoad(Node):
    """Load data into a CSV from a Pandas DataFrame"""

    def begin(self):
        """Initialize state for CSV writing"""
        self.wrote_header = False

    def run(self, df, f, push_file=False, dry_run=False, **kwargs):
        """Use Pandas to_csv to output a DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to load to a CSV
        f : file or buffer
            File to write the DataFrame to
        push_file : bool, optional
            If true, push the file forward instead of the data
        dry_run : bool, optional
            If true, skip actually loading the data
        **kwargs
            Keyword arguments passed to DataFrame.to_csv

        """
        raiseifnot(pd, "Please install Pandas to use this class")

        if "header" not in kwargs:
            kwargs["header"] = not self.wrote_header
            if not self.wrote_header:
                self.wrote_header = True

        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            df.to_csv(f, **kwargs)

        if push_file:
            self.push(f)
        else:
            self.push(df)

    def end(self):
        """Reset state in case the node gets reused"""
        self.wrote_header = False


class DataFrameExcelLoad(Node):
    """Load data into an Excel file from a Pandas DataFrame"""

    def run(self, df_or_dict, f, push_file=False, dry_run=False, **kwargs):
        """Use Pandas to_excel to output a DataFrame

        Parameters
        ----------
        df_or_dict
            DataFrame or dict of DataFrames to load to an Excel file. In the
            case of a dict the keys will be the sheet names.
        f : file or buffer
            File to write the DataFrame to
        push_file : bool, optional
            If true, push the file forward instead of the data
        dry_run : bool, optional
            If true, skip actually loading the data
        **kwargs
            Keyword arguments passed to DataFrame.to_excel

        """
        raiseifnot(pd, "Please install Pandas to use this class")

        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            if isinstance(df_or_dict, dict):
                with pd.ExcelWriter(f) as writer:
                    for sheet_name, df in df_or_dict.items():
                        df.to_excel(writer, sheet_name=sheet_name)
            else:
                df_or_dict.to_excel(f, **kwargs)

        if push_file:
            self.push(f)
        else:
            self.push(df_or_dict)


class DataFrameSQLLoad(PandasSQLNode):
    """Load data into a SQL db from a Pandas DataFrame"""

    def run(self, df, conn, table, push_table=False, dry_run=False, **kwargs):
        """Use Pandas to_sql to output a DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to load to a SQL table
        conn
            Database connection
        table : str
            Name of a table to write the data to
        push_table : bool, optional
            If true, push the table forward instead of the data
        dry_run : bool, optional
            If true, skip actually loading the data
        **kwargs
            Keyword arguments passed to DataFrame.to_sql

        """
        raiseifnot(pd, "Please install Pandas to use this class")

        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            df.to_sql(table, conn, **kwargs)

        if push_table:
            self.push(table)
        else:
            self.push(df)


class DataFrameSQLTempLoad(PandasSQLNode):
    """Load data into a SQL temp table from a Pandas DataFrame"""

    def run(self, df, conn, schema=None, dry_run=False, **kwargs):
        """Use Pandas to_sql to output a DataFrame to a temporary table. Push a
        reference to the temp table forward.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to load to a SQL table
        conn
            Database connection
        schema : str, optional
            schema to create the temp table in
        dry_run : bool, optional
            If true, skip actually loading the data
        **kwargs
            Keyword arguments passed to DataFrame.to_sql

        """
        raiseifnot(pd, "Please install Pandas to use this class")

        raiseif(
            isinstance(conn, sqlite3.Connection),
            "sqlite3 connections not supported due to bug in Pandas' has_table()",
        )

        table = get_temp_table(conn, df, schema=schema, create=True)
        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            df.to_sql(table.name, conn, if_exists="append", **kwargs)

        self.push(table.name)


class DataFrameHTMLLoad(Node):
    def run(self, df, f, push_file=False, dry_run=False, **kwargs):
        """Use Pandas to_html to output a DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to load to an HTML file
        f : file or buffer
            File to write the DataFrame to
        push_file : bool, optional
            If true, push the file forward instead of the data
        dry_run : bool, optional
            If true, skip actually loading the data
        **kwargs
            Keyword arguments passed to DataFrame.to_html

        """
        raiseifnot(pd, "Please install Pandas to use this class")

        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            df.to_html(f, **kwargs)

        if push_file:
            self.push(f)
        else:
            self.push(df)


class DataFrameApplyMap(Node):
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
        raiseifnot(pd, "Please install Pandas to use this class")
        df = df.applymap(func, **kwargs)
        self.push(df)


class DataFrameRollingNode(Node):
    """Apply df.rolling to a DataFrame"""

    def compute_stats(self, df, rolling, column_name):
        """Override this to implement logic to manipulate the DataFrame"""
        raise NotImplementedError

    def run(self, df, windows, columns=None, suffix=None, **kwargs):
        """Use df.rolling to apply a rolling window calculation on a dataframe

        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rolling.html

        Parameters
        ----------
        df : pandas.DataFrame
            The pandas DataFrame to process
        windows : int or list of ints
            Size(s) of the moving window(s). If a list, all windows will be
            calculated and the window size will be appended as a suffix.
        columns : list, optional
            A list of columns to calculate values for
        suffix : str, optional
            A suffix to add to the column names of calculated values
        **kwargs
            Keyword arguments passed to df.rolling

        """
        columns = columns or df.columns
        windows = listify(windows)

        for column in columns:
            for window in windows:
                rolling = df[column].rolling(window, **kwargs)

                column_name = column
                if suffix or len(windows) > 1:
                    window_suffix = suffix or ""
                    if len(windows) > 1:
                        window_suffix = "%s-%s" % (window_suffix, window)
                    column_name = column + window_suffix

                self.compute_stats(df, rolling, column_name)

        self.push(df)


class DataFrameMovingAverage(DataFrameRollingNode):
    """Compute a moving average on a DataFrame"""

    def compute_stats(self, df, rolling, column_name):
        df[column_name] = rolling.mean()


class DataFrameRollingSum(DataFrameRollingNode):
    """Compute a rolling window sum on a DataFrame"""

    def compute_stats(self, df, rolling, column_name):
        df[column_name] = rolling.sum()


class DataFrameRollingStd(DataFrameRollingNode):
    """Compute a rolling standard deviation on a DataFrame"""

    def compute_stats(self, df, rolling, column_name):
        df[column_name] = rolling.std()


class DataFrameBollingerBands(DataFrameRollingNode):
    """Compute bollinger bands for the specified columns in a DataFrame"""

    def compute_stats(self, df, rolling, column_name):
        df_ma = rolling.mean()
        df_std = rolling.std()
        upper = df_ma + 2 * df_std
        lower = df_ma - 2 * df_std
        df[column_name] = df_ma
        df[column_name + "-lower"] = lower
        df[column_name + "-upper"] = upper
