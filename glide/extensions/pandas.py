"""https://pandas.pydata.org/"""

try:
    import pandas as pd
except ImportError:
    pd = None
import sqlite3
from tlbx import st

from glide.core import Node, BaseSQLNode
from glide.sql_utils import build_table_select, get_temp_table, SQLALCHEMY_CONN_TYPES
from glide.utils import warn


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
        assert pd, "Please install Pandas to use this class"
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
        assert pd, "Please install Pandas to use this class"
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
        assert pd, "Please install Pandas to use this class"
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
        assert pd, "Please install Pandas to use this class"
        sql = build_table_select(table, where=where, limit=limit)
        df = pd.read_sql(sql, conn, **kwargs)
        self.do_push(df, kwargs.get("chunksize", None))


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
        assert pd, "Please install Pandas to use this class"

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
        assert pd, "Please install Pandas to use this class"

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
        assert pd, "Please install Pandas to use this class"

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
        assert pd, "Please install Pandas to use this class"

        assert not isinstance(
            conn, sqlite3.Connection
        ), "sqlite3 connections not supported due to bug in Pandas' has_table()?"

        table = get_temp_table(conn, df, schema=schema, create=True)
        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            df.to_sql(table.name, conn, if_exists="append", **kwargs)

        self.push(table.name)


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
        assert pd, "Please install Pandas to use this class"
        df = df.applymap(func, **kwargs)
        self.push(df)
