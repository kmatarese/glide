"""A home for common data load nodes"""

from copy import deepcopy
import csv
import sqlite3

import pandas as pd
import requests
from toolbox import st, pp, dbg

from glide.core import (
    Node,
    SQLConnectionNode,
    PandasSQLConnectionNode,
    SQLAlchemyConnectionNode,
    SQLDBAPIConnectionNode,
    SQLiteConnectionNode,
)
from glide.sql_utils import (
    TemporaryTable,
    SQLiteTemporaryTable,
    get_bulk_replace,
    get_bulk_insert,
    get_temp_table,
)


class Logger(Node):
    """Simple logging node"""

    def run(self, item):
        """Pretty print the item and then push"""
        print("---- %s ----" % self.name)
        pp(item)
        self.push(item)


# -------- Pandas Loaders


class DataFrameCSVLoader(Node):
    """Load data into a CSV from a Pandas DataFrame"""

    def begin(self):
        """Initialize state for CSV writing"""
        self.wrote_header = False

    def run(self, df, f, **kwargs):
        """Use Pandas to_csv to output a DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to load to a CSV
        f : file or buffer
            File to write the DataFrame to
        **kwargs
            Keyword arguments passed to DataFrame.to_csv

        """
        if "header" not in kwargs:
            kwargs["header"] = not self.wrote_header
            if not self.wrote_header:
                self.wrote_header = True
        df.to_csv(f, **kwargs)
        self.push(df)

    def end(self):
        """Reset state in case the node gets reused"""
        self.wrote_header = False


class DataFrameExcelLoader(Node):
    """Load data into an Excel file from a Pandas DataFrame"""

    def run(self, df, f, **kwargs):
        """Use Pandas to_excel to output a DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to load to an Excel file
        f : file or buffer
            File to write the DataFrame to
        **kwargs
            Keyword arguments passed to DataFrame.to_excel

        """
        df.to_excel(f, **kwargs)
        self.push(df)


class DataFrameSQLLoader(PandasSQLConnectionNode):
    """Load data into a SQL db from a Pandas DataFrame"""

    def run(self, df, conn, table, **kwargs):
        """Use Pandas to_sql to output a DataFrame

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to load to a SQL table
        conn
            Database connection
        table
            Name of a table to write the data to
        **kwargs
            Keyword arguments passed to DataFrame.to_sql

        """
        df.to_sql(table, conn, **kwargs)
        self.push(df)


class DataFrameSQLTempLoader(PandasSQLConnectionNode):
    """Load data into a SQL temp table from a Pandas DataFrame"""

    def run(self, df, conn, schema=None, **kwargs):
        """Use Pandas to_sql to output a DataFrame to a temporary table.

        Note: this pushes the temporary table name instead of the data.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to load to a SQL table
        conn
            Database connection
        schema
            schema to create the temp table in
        **kwargs
            Keyword arguments passed to DataFrame.to_sql

        """
        assert not isinstance(
            conn, sqlite3.Connection
        ), "sqlite3 connections not supported due to bug in Pandas' has_table()?"
        table = get_temp_table(conn, df, schema=schema, create=True)
        df.to_sql(table.name, conn, if_exists="append", **kwargs)
        self.push([table.name])


# -------- Row-based Loaders


class RowCSVLoader(Node):
    """Load data into a CSV using DictWriter"""

    def begin(self):
        """Initialize state for CSV writing"""
        self.writer = None

    def run(self, rows, f, **kwargs):
        """Use DictWriter to output dict rows to a CSV.

        Parameters
        ----------
        row
            Iterable of rows to load to a CSV
        f : file or buffer
            File to write rows to
        **kwargs
            Keyword arguments passed to csv.DictWriter

        """
        close = False
        if isinstance(f, str):
            f = open(f, "w")
            close = True

        try:
            if not self.writer:
                self.writer = csv.DictWriter(f, fieldnames=rows[0].keys(), **kwargs)
                self.writer.writeheader()
            self.writer.writerows(rows)
        finally:
            if close:
                f.close()
        self.push(rows)

    def end(self):
        """Reset state in case the node gets reused"""
        self.writer = None


class RowSQLiteLoader(SQLiteConnectionNode):
    """Load date with a SQLite connection"""

    def run(self, rows, conn, table, cursor=None, commit=True, **kwargs):
        """Form SQL statement and use bulk execute to write rows to table

        Parameters
        ----------
        rows
            Iterable of rows to load to the table
        conn
            SQLite database connection
        table
            Name of a table to write the data to
        cursor : optional
            SQLite database cursor
        commit : bool, optional
            If true call conn.commit
        **kwargs
            Keyword arguments passed to cursor.executemany

        """

        dbg("Loading %d rows to %s" % (len(rows), table))
        assert isinstance(rows[0], sqlite3.Row), "Only sqlite3.Row rows are supported"
        sql = get_bulk_replace(table, rows[0].keys(), dicts=False, value_string="?")
        if not cursor:
            cursor = conn.cursor()
        cursor.executemany(sql, rows)
        if commit:
            conn.commit()
        self.push(rows)


class RowSQLDBAPILoader(SQLDBAPIConnectionNode):
    """Load data with a DBAPI-based connection"""

    def run(self, rows, conn, table, cursor=None, commit=True, **kwargs):
        """Form SQL statement and use bulk execute to write rows to table

        Parameters
        ----------
        rows
            Iterable of rows to load to the table
        conn
            DBAPI database connection
        table
            Name of a table to write the data to
        cursor : optional
            DBAPI database connection cursor
        commit : bool, optional
            If true call conn.commit
        **kwargs
            Keyword arguments passed to cursor.executemany

        """
        dbg("Loading %d rows to %s" % (len(rows), table))
        sql = get_bulk_replace(table, rows[0].keys())
        if not cursor:
            cursor = conn.cursor()
        cursor.executemany(sql, rows)
        if commit and hasattr(conn, "commit"):
            conn.commit()
        self.push(rows)


class RowSQLAlchemyLoader(SQLAlchemyConnectionNode):
    """Load data with a SQLAlchemy connection"""

    def run(self, rows, conn, table):
        """Form SQL statement and use bulk execute to write rows to table

        Parameters
        ----------
        rows
            Iterable of rows to load to the table
        conn
            SQLAlchemy database connection
        table
            Name of a table to write the data to

        """
        dbg("Loading %d rows to %s" % (len(rows), table))
        sql = get_bulk_replace(table, rows[0].keys(), dicts=False)
        conn.execute(sql, rows)
        self.push(rows)


class RowSQLiteTempLoader(SQLiteConnectionNode):
    """Load data into a temp table with a SQLite connection"""

    def run(self, rows, conn, cursor=None, schema=None, commit=True, **kwargs):
        """Create and bulk load a temp table

        Parameters
        ----------
        rows
            Iterable of rows to load to the table
        conn
            SQLite database connection
        cursor : optional
            SQLite database cursor
        schema : str, optional
            Schema to create temp table in
        commit : bool, optional
            If true call conn.commit
        **kwargs
            Keyword arguments passed to cursor.executemany

        """
        assert isinstance(rows[0], sqlite3.Row), "Only sqlite3.Row rows are supported"
        table = get_temp_table(conn, rows, create=True, schema=schema)
        sql = get_bulk_replace(
            table.name, rows[0].keys(), dicts=False, value_string="?"
        )
        if not cursor:
            cursor = conn.cursor()
        cursor.executemany(sql, rows)
        if commit:
            conn.commit()
        self.push([table.name])


class RowSQLLoader(SQLConnectionNode):
    """Generic SQL loader"""

    def run(self, rows, conn, table, cursor=None, commit=True):
        """Form SQL statement and use bulk execute to write rows to table

        Parameters
        ----------
        rows
            Iterable of rows to load to the table
        conn
            Database connection
        table
            Name of a table to write the data to
        cursor : optional
            Database connection cursor
        commit : bool, optional
            If true and conn has a commit method, call conn.commit

        """
        dbg("Loading %d rows to %s" % (len(rows), table))
        sql = self.get_bulk_replace(conn, table, rows)
        if not cursor:
            cursor = self.get_sql_executor(conn)
        self.sql_executemany(conn, cursor, sql, rows)
        if commit and hasattr(conn, "commit"):
            conn.commit()
        self.push(rows)


class RowSQLTempLoader(SQLConnectionNode):
    """Generic SQL temp table loader"""

    def run(self, rows, conn, cursor=None, schema=None, commit=True):
        """Create and bulk load a temp table

        Parameters
        ----------
        rows
            Iterable of rows to load to the table
        conn
            Database connection
        cursor : optional
            Database connection cursor
        schema : str, optional
            Schema to create temp table in
        commit : bool, optional
            If true and conn has a commit method, call conn.commit

        """
        table = get_temp_table(conn, rows, create=True, schema=schema)
        sql = self.get_bulk_replace(conn, table.name, rows)
        if not cursor:
            cursor = self.get_sql_executor(conn)
        self.sql_executemany(conn, cursor, sql, rows)
        if commit and hasattr(conn, "commit"):
            conn.commit()
        self.push([table.name])


# -------- Other Loaders


class URLLoader(Node):
    """Load data to URL with requests"""

    def run(
        self,
        data,
        url,
        data_param="data",
        session=None,
        raise_for_status=True,
        **kwargs
    ):
        """Load data to URL using requests and push response.content. The url maybe be
        a string (POST that url) or a dictionary of args to requests.request:

        http://2.python-requests.org/en/master/api/?highlight=get#requests.request

        Parameters
        ----------
        data
            Data to load to the URL
        url : str or dict
            If str, a URL to POST to. If a dict, args to requets.request
        data_param : str, optional
            parameter to stuff data in when calling requests methods
        session : optional
            A requests Session to use to make the request
        raise_for_status : bool, optional
            Raise exceptions for bad response status
        **kwargs
            Keyword arguments to pass to the request method. If a dict is
            passed for the url parameter it overrides values here.

        """
        requestor = requests
        if session:
            requestor = session

        if isinstance(url, str):
            assert not (
                "data" in kwargs or "json" in kwargs
            ), "Overriding data/json params is not allowed"
            kwargs[data_param] = data
            resp = requestor.post(url, **kwargs)
        elif isinstance(url, dict):
            kwargs_copy = deepcopy(kwargs)
            kwargs_copy.update(url)
            assert not (
                "data" in kwargs_copy or "json" in kwargs_copy
            ), "Overriding data/json params is not allowed"
            kwargs_copy[data_param] = data
            resp = requestor.request(**kwargs_copy)
        else:
            assert False, "Input url must be a str or dict type, got %s" % type(url)

        if raise_for_status:
            resp.raise_for_status()

        self.push(data)
