"""
To support email:
https://stackoverflow.com/questions/51062176/pandas-read-data-from-a-secure-ftp-server-in-python-3

SFTP is already supported by passing a file-like object to pandas:
https://stackoverflow.com/questions/51062176/pandas-read-data-from-a-secure-ftp-server-in-python-3

Pandas has S3 support as well:
https://stackoverflow.com/a/51777553/10682164
"""

import csv

import pandas as pd

from consecutils.core import (
    Node,
    DataFramePushNode,
    SQLConnectionNode,
    PandasSQLConnectionNode,
    SQLAlchemyConnectionNode,
    SQLDBAPIConnectionNode,
    SQLiteConnectionNode,
)
from consecutils.utils import read_chunks
from consecutils.sql_utils import build_table_select

# -------- Pandas Extractors


class DataFrameCSVExtractor(DataFramePushNode):
    def run(self, files, **kwargs):
        for f in files:
            df = pd.read_csv(f, **kwargs)
            self.do_push(df, **kwargs)


class DataFrameExcelExtractor(DataFramePushNode):
    def run(self, files, **kwargs):
        for f in files:
            df = pd.read_excel(f, **kwargs)
            self.do_push(df, **kwargs)


class DataFrameSQLExtractor(PandasSQLConnectionNode):
    def run(self, queries, conn, **kwargs):
        for sql in queries:
            df = pd.read_sql(sql, conn, **kwargs)
            self.do_push(df, **kwargs)


class DataFrameSQLTableExtractor(PandasSQLConnectionNode):
    def run(self, tables, conn, where=None, limit=None, **kwargs):
        for table in tables:
            sql = build_table_select(table, where=where, limit=limit)
            df = pd.read_sql(sql, conn, **kwargs)
            self.do_push(df, **kwargs)


# -------- Row-based Extractors


class RowCSVExtractor(Node):
    def run(self, files, chunksize=None, nrows=None, **kwargs):
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
    def run(self, queries, conn, cursor=None, chunksize=None, **kwargs):
        if not cursor:
            cursor = conn.cursor()
        for sql in queries:
            qr = cursor.execute(sql, **kwargs)
            self.do_push(cursor, chunksize=chunksize)


class RowSQLDBAPIExtractor(SQLDBAPIConnectionNode):
    def run(self, queries, conn, cursor=None, chunksize=None, **kwargs):
        if not cursor:
            cursor = conn.cursor()
        for sql in queries:
            qr = cursor.execute(sql, **kwargs)
            self.do_push(cursor, chunksize=chunksize)


class RowSQLAlchemyExtractor(SQLAlchemyConnectionNode):
    def run(self, queries, conn, chunksize=None, **kwargs):
        for sql in queries:
            qr = conn.execute(sql, **kwargs)
            self.do_push(qr, chunksize=chunksize)


class RowSQLiteTableExtractor(SQLiteConnectionNode):
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
        if not cursor:
            cursor = conn.cursor()
        for table in tables:
            sql = build_table_select(table, where=where, limit=limit)
            qr = cursor.execute(sql, **kwargs)
            self.do_push(cursor, chunksize=chunksize)


class RowSQLDBAPITableExtractor(SQLDBAPIConnectionNode):
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
        if not cursor:
            cursor = conn.cursor()
        for table in tables:
            sql = build_table_select(table, where=where, limit=limit)
            qr = cursor.execute(sql, **kwargs)
            self.do_push(cursor, chunksize=chunksize)


class RowSQLAlchemyTableExtractor(SQLAlchemyConnectionNode):
    def run(self, tables, conn, where=None, limit=None, chunksize=None, **kwargs):
        for table in tables:
            sql = build_table_select(table, where=where, limit=limit)
            qr = conn.execute(sql, **kwargs)
            self.do_push(qr, chunksize=chunksize)


class RowSQLExtractor(SQLConnectionNode):
    """Generic SQL Extractor"""

    def run(self, queries, conn, cursor=None, chunksize=None, **kwargs):
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
        if not cursor:
            cursor = self.get_sql_executor(conn)
        for table in tables:
            sql = build_table_select(table, where=where, limit=limit)
            fetcher = self.sql_execute(conn, cursor, sql, **kwargs)
            self.do_push(fetcher, chunksize=chunksize)
