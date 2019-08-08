"""A home for common data extraction nodes

To support email:
http://code.activestate.com/recipes/576858-send-html-or-text-email-with-or-without-attachment/

SFTP is already supported by passing a file-like object to pandas:
https://stackoverflow.com/questions/51062176/pandas-read-data-from-a-secure-ftp-server-in-python-3

Pandas has S3 support as well:
https://stackoverflow.com/a/51777553/10682164
"""

import csv

import pandas as pd

from glide.core import (
    Node,
    DataFramePushNode,
    SQLConnectionNode,
    PandasSQLConnectionNode,
    SQLAlchemyConnectionNode,
    SQLDBAPIConnectionNode,
    SQLiteConnectionNode,
)
from glide.utils import read_chunks
from glide.sql_utils import build_table_select

# -------- Pandas Extractors


class DataFrameCSVExtractor(DataFramePushNode):
    """Extract data from a CSV using Pandas"""
    def run(self, files, **kwargs):
        """Extract data for each input file and push as a DataFrame"""
        for f in files:
            df = pd.read_csv(f, **kwargs)
            self.do_push(df, **kwargs)


class DataFrameExcelExtractor(DataFramePushNode):
    """Extract data from an Excel file using Pandas"""
    def run(self, files, **kwargs):
        """Extract data for each input file and push as a DataFrame"""
        for f in files:
            df = pd.read_excel(f, **kwargs)
            self.do_push(df, **kwargs)


class DataFrameSQLExtractor(PandasSQLConnectionNode):
    """Extract data from a SQL db using Pandas"""
    def run(self, queries, conn, **kwargs):
        """Extract data for each input query and push as a DataFrame"""
        for sql in queries:
            df = pd.read_sql(sql, conn, **kwargs)
            self.do_push(df, **kwargs)


class DataFrameSQLTableExtractor(PandasSQLConnectionNode):
    """Extract data from a SQL table using Pandas"""
    def run(self, tables, conn, where=None, limit=None, **kwargs):
        """Extract data for each input table and push as a DataFrame"""
        for table in tables:
            sql = build_table_select(table, where=where, limit=limit)
            df = pd.read_sql(sql, conn, **kwargs)
            self.do_push(df, **kwargs)


# -------- Row-based Extractors


class RowCSVExtractor(Node):
    """Extract data from a CSV using DictReader"""
    def run(self, files, chunksize=None, nrows=None, **kwargs):
        """Extract data for each input file and push dict rows"""
        for f in files:
            close = False
            if isinstance(f, str):
                f = open(f, "r")
                close = True

            try:
                reader = csv.DictReader(f, **kwargs)

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
                    f.close()


class RowSQLiteExtractor(SQLiteConnectionNode):
    """Extract data from a SQLite connection"""
    def run(self, queries, conn, cursor=None, chunksize=None, **kwargs):
        """Extract data for each input query and push fetched rows"""
        if not cursor:
            cursor = conn.cursor()
        for sql in queries:
            qr = cursor.execute(sql, **kwargs)
            self.do_push(cursor, chunksize=chunksize)


class RowSQLDBAPIExtractor(SQLDBAPIConnectionNode):
    """Extract data from a DBAPI connection"""
    def run(self, queries, conn, cursor=None, chunksize=None, **kwargs):
        """Extract data for each input query and push fetched rows"""
        if not cursor:
            cursor = conn.cursor()
        for sql in queries:
            qr = cursor.execute(sql, **kwargs)
            self.do_push(cursor, chunksize=chunksize)


class RowSQLAlchemyExtractor(SQLAlchemyConnectionNode):
    """Extract data from a SQLAlchemy connection"""
    def run(self, queries, conn, chunksize=None, **kwargs):
        """Extract data for each input query and push fetched rows"""
        for sql in queries:
            qr = conn.execute(sql, **kwargs)
            self.do_push(qr, chunksize=chunksize)


class RowSQLiteTableExtractor(SQLiteConnectionNode):
    """Extract data from SQLite tables"""
    def run(
        self,
        tables,
        conn,
        cursor=None,
        where=None,
        limit=None,
        chunksize=None,
        **kwargs
    ):
        """Extract data for each input table and push fetched rows"""
        if not cursor:
            cursor = conn.cursor()
        for table in tables:
            sql = build_table_select(table, where=where, limit=limit)
            qr = cursor.execute(sql, **kwargs)
            self.do_push(cursor, chunksize=chunksize)


class RowSQLDBAPITableExtractor(SQLDBAPIConnectionNode):
    """Extract data from tables in a DBAPI connected"""
    def run(
        self,
        tables,
        conn,
        cursor=None,
        where=None,
        limit=None,
        chunksize=None,
        **kwargs
    ):
        """Extract data for each input table and push fetched rows"""
        if not cursor:
            cursor = conn.cursor()
        for table in tables:
            sql = build_table_select(table, where=where, limit=limit)
            qr = cursor.execute(sql, **kwargs)
            self.do_push(cursor, chunksize=chunksize)


class RowSQLAlchemyTableExtractor(SQLAlchemyConnectionNode):
    """Extract data from tables in a SQLAlchemy connection"""
    def run(self, tables, conn, where=None, limit=None, chunksize=None, **kwargs):
        """Extract data for each input table and push fetched rows"""
        for table in tables:
            sql = build_table_select(table, where=where, limit=limit)
            qr = conn.execute(sql, **kwargs)
            self.do_push(qr, chunksize=chunksize)


class RowSQLExtractor(SQLConnectionNode):
    """Generic SQL Extractor"""

    def run(self, queries, conn, cursor=None, chunksize=None, **kwargs):
        """Extract data for each input query and push fetched rows"""
        if not cursor:
            cursor = self.get_sql_executor(conn)
        for sql in queries:
            fetcher = self.sql_execute(conn, cursor, sql, **kwargs)
            self.do_push(fetcher, chunksize=chunksize)


class RowSQLTableExtractor(SQLConnectionNode):
    """Generic SQL Table Extractor"""

    def run(
        self,
        tables,
        conn,
        cursor=None,
        where=None,
        limit=None,
        chunksize=None,
        **kwargs
    ):
        """Extract data for each input table and push fetched rows"""
        if not cursor:
            cursor = self.get_sql_executor(conn)
        for table in tables:
            sql = build_table_select(table, where=where, limit=limit)
            fetcher = self.sql_execute(conn, cursor, sql, **kwargs)
            self.do_push(fetcher, chunksize=chunksize)
