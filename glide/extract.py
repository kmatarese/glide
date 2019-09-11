"""A home for common data extraction nodes

To support email:
http://code.activestate.com/recipes/576858-send-html-or-text-email-with-or-without-attachment/

SFTP is already supported by passing a file-like object to pandas:
https://stackoverflow.com/questions/51062176/pandas-read-data-from-a-secure-ftp-server-in-python-3

Pandas has S3 support as well:
https://stackoverflow.com/a/51777553/10682164
"""

import codecs
from copy import deepcopy
import csv
from io import BytesIO

import pandas as pd
from pandas.io.common import get_filepath_or_buffer
import requests
from toolbox import st, read_chunks

from glide.core import (
    Node,
    DataFramePushNode,
    SQLConnectionNode,
    PandasSQLConnectionNode,
    SQLAlchemyConnectionNode,
    SQLDBAPIConnectionNode,
    SQLiteConnectionNode,
)
from glide.sql_utils import build_table_select

# -------- Pandas Extractors


class DataFrameCSVExtractor(DataFramePushNode):
    """Extract data from a CSV using Pandas"""

    def run(self, f, **kwargs):
        """Extract data for input file and push as a DataFrame"""
        df = pd.read_csv(f, **kwargs)
        self.do_push(df, **kwargs)


class DataFrameExcelExtractor(DataFramePushNode):
    """Extract data from an Excel file using Pandas"""

    def run(self, f, **kwargs):
        """Extract data for input file and push as a DataFrame"""
        df = pd.read_excel(f, **kwargs)
        self.do_push(df, **kwargs)


class DataFrameSQLExtractor(PandasSQLConnectionNode):
    """Extract data from a SQL db using Pandas"""

    def run(self, sql, conn, **kwargs):
        """Extract data for input query and push as a DataFrame"""
        df = pd.read_sql(sql, conn, **kwargs)
        self.do_push(df, **kwargs)


class DataFrameSQLTableExtractor(PandasSQLConnectionNode):
    """Extract data from a SQL table using Pandas"""

    def run(self, table, conn, where=None, limit=None, **kwargs):
        """Extract data for input table and push as a DataFrame"""
        sql = build_table_select(table, where=where, limit=limit)
        df = pd.read_sql(sql, conn, **kwargs)
        self.do_push(df, **kwargs)


# -------- Row-based Extractors


class RowCSVExtractor(Node):
    """Extract data from a CSV"""

    def run(self, f, chunksize=None, nrows=None, reader=csv.DictReader, **kwargs):
        """Extract data for input file and push dict rows"""

        # Re-use pandas functionality utilized by read_csv
        # TODO: this uses urlopen under the hood. It may be more efficient to use
        # requests.get() with stream=True.
        # https://stackoverflow.com/a/42979967/10682164
        f, encoding, compression, should_close = get_filepath_or_buffer(f)

        close = False or should_close
        decode = False
        if isinstance(f, str):
            f = open(f, "r")
            close = True
        elif isinstance(f, BytesIO) or encoding:
            decode = True

        try:
            if decode:
                reader = reader(codecs.iterdecode(f, encoding or "utf-8"), **kwargs)
            else:
                reader = reader(f, **kwargs)

            if chunksize:
                for chunk in read_chunks(reader, chunksize, limit=nrows):
                    self.push(chunk)
            else:
                rows = []
                for i, row in enumerate(reader):
                    if nrows and i >= nrows:
                        break
                    rows.append(row)
                self.push(rows)
        finally:
            if close:
                try:
                    f.close()
                except ValueError:
                    pass


class RowSQLiteExtractor(SQLiteConnectionNode):
    """Extract data from a SQLite connection"""

    def run(self, sql, conn, cursor=None, chunksize=None, **kwargs):
        """Extract data for input query and push fetched rows"""
        if not cursor:
            cursor = conn.cursor()
        qr = cursor.execute(sql, **kwargs)
        self.do_push(cursor, chunksize=chunksize)


class RowSQLDBAPIExtractor(SQLDBAPIConnectionNode):
    """Extract data from a DBAPI connection"""

    def run(self, sql, conn, cursor=None, chunksize=None, **kwargs):
        """Extract data for input query and push fetched rows"""
        if not cursor:
            cursor = conn.cursor()
        qr = cursor.execute(sql, **kwargs)
        self.do_push(cursor, chunksize=chunksize)


class RowSQLAlchemyExtractor(SQLAlchemyConnectionNode):
    """Extract data from a SQLAlchemy connection"""

    def run(self, sql, conn, chunksize=None, **kwargs):
        """Extract data for each input query and push fetched rows"""
        qr = conn.execute(sql, **kwargs)
        self.do_push(qr, chunksize=chunksize)


class RowSQLiteTableExtractor(SQLiteConnectionNode):
    """Extract data from SQLite tables"""

    def run(
        self, table, conn, cursor=None, where=None, limit=None, chunksize=None, **kwargs
    ):
        """Extract data for each input table and push fetched rows"""
        if not cursor:
            cursor = conn.cursor()
        sql = build_table_select(table, where=where, limit=limit)
        qr = cursor.execute(sql, **kwargs)
        self.do_push(cursor, chunksize=chunksize)


class RowSQLDBAPITableExtractor(SQLDBAPIConnectionNode):
    """Extract data from tables in a DBAPI connected"""

    def run(
        self, table, conn, cursor=None, where=None, limit=None, chunksize=None, **kwargs
    ):
        """Extract data for each input table and push fetched rows"""
        if not cursor:
            cursor = conn.cursor()
        sql = build_table_select(table, where=where, limit=limit)
        qr = cursor.execute(sql, **kwargs)
        self.do_push(cursor, chunksize=chunksize)


class RowSQLAlchemyTableExtractor(SQLAlchemyConnectionNode):
    """Extract data from tables in a SQLAlchemy connection"""

    def run(self, table, conn, where=None, limit=None, chunksize=None, **kwargs):
        """Extract data for each input table and push fetched rows"""
        sql = build_table_select(table, where=where, limit=limit)
        qr = conn.execute(sql, **kwargs)
        self.do_push(qr, chunksize=chunksize)


class RowSQLExtractor(SQLConnectionNode):
    """Generic SQL Extractor"""

    def run(self, sql, conn, cursor=None, chunksize=None, **kwargs):
        """Extract data for each input query and push fetched rows"""
        if not cursor:
            cursor = self.get_sql_executor(conn)
        fetcher = self.sql_execute(conn, cursor, sql, **kwargs)
        self.do_push(fetcher, chunksize=chunksize)


class RowSQLTableExtractor(SQLConnectionNode):
    """Generic SQL Table Extractor"""

    def run(
        self, table, conn, cursor=None, where=None, limit=None, chunksize=None, **kwargs
    ):
        """Extract data for each input table and push fetched rows"""
        if not cursor:
            cursor = self.get_sql_executor(conn)
        sql = build_table_select(table, where=where, limit=limit)
        fetcher = self.sql_execute(conn, cursor, sql, **kwargs)
        self.do_push(fetcher, chunksize=chunksize)


# -------- Other Extractors


class URLExtractor(Node):
    """Extract data from a URL with requests"""

    def run(
        self,
        url,
        response_type="content",
        session=None,
        raise_for_status=True,
        **kwargs
    ):
        """Extract data from a URL using requests and push response.content.  Input
        url maybe be a string (GET that url) or a dictionary of args to
        requests.request:

        http://2.python-requests.org/en/master/api/?highlight=get#requests.request

        """
        requestor = requests
        if session:
            requestor = session

        if isinstance(url, str):
            resp = requestor.get(url, **kwargs)
        elif isinstance(url, dict):
            kwargs_copy = deepcopy(kwargs)
            kwargs_copy.update(url)
            resp = requestor.request(**kwargs_copy)
        else:
            assert False, "Input url must be a str or dict type, got %s" % type(url)

        if raise_for_status:
            resp.raise_for_status()

        if response_type == "content":
            data = resp.content
        elif response_type == "text":
            data = resp.text
        elif response_type == "json":
            data = resp.json()
        else:
            assert False, (
                "Unrecognized response_type: %s, must be one of content, text, or json"
                % response_type
            )

        self.push(data)
