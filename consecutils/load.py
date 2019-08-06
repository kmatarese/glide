import csv
import sqlite3

import pandas as pd

from consecutils.core import (
    Node,
    SQLConnectionNode,
    PandasSQLConnectionNode,
    SQLAlchemyConnectionNode,
    SQLDBAPIConnectionNode,
    SQLiteConnectionNode,
)
from consecutils.utils import st, pp
from consecutils.sql_utils import (
    TemporaryTable,
    SQLiteTemporaryTable,
    get_bulk_replace,
    get_bulk_insert,
    get_temp_table,
)


class LoggingLoader(Node):
    def run(self, item):
        print("---- %s ----" % self.name)
        pp(item)
        self.push(item)


# -------- Pandas Loaders


class DataFrameCSVLoader(Node):
    def begin(self):
        self.wrote_header = False

    def run(self, df, outfile, **kwargs):
        if "header" not in kwargs:
            kwargs["header"] = not self.wrote_header
            if not self.wrote_header:
                self.wrote_header = True
        df.to_csv(outfile, **kwargs)
        self.push(df)

    def end(self):
        # Reset this in case the node gets reused in a pipeline
        self.wrote_header = False


class DataFrameExcelLoader(Node):
    def run(self, df, outfile, **kwargs):
        df.to_excel(outfile, **kwargs)
        self.push(df)


class DataFrameSQLLoader(PandasSQLConnectionNode):
    def run(self, df, conn, table, **kwargs):
        df.to_sql(table, conn, **kwargs)
        self.push(df)


class DataFrameSQLTempLoader(PandasSQLConnectionNode):
    def run(self, df, conn, schema=None, **kwargs):
        assert not isinstance(
            conn, sqlite3.Connection
        ), "sqlite3 connections not supported due to bug in Pandas' has_table()?"
        table = get_temp_table(conn, df, schema=schema, create=True)
        df.to_sql(table.name, conn, if_exists="append", **kwargs)
        self.push([table.name])


# -------- Row-based Loaders


class RowCSVLoader(Node):
    def begin(self):
        self.writer = None

    def run(self, rows, outfile, **kwargs):
        # Assumes its an open file, since opening it per row wouldn't be great.
        if not self.writer:
            self.writer = csv.DictWriter(outfile, fieldnames=rows[0].keys(), **kwargs)
            self.writer.writeheader()
        self.writer.writerows(rows)
        self.push(rows)

    def end(self):
        # Reset this in case the node gets reused in a pipeline
        self.writer = None


class RowSQLiteLoader(SQLiteConnectionNode):
    def run(self, rows, conn, table, cursor=None, **kwargs):
        assert isinstance(rows[0], sqlite3.Row), "Only sqlite3.Row rows are supported"
        sql = get_bulk_replace(table, rows[0].keys(), dicts=False, value_string="?")
        if not cursor:
            cursor = conn.cursor()
        cursor.executemany(sql, rows)
        self.push(rows)


class RowSQLDBAPILoader(SQLDBAPIConnectionNode):
    def run(self, rows, conn, table, cursor=None, **kwargs):
        sql = get_bulk_replace(table, rows[0].keys())
        if not cursor:
            cursor = conn.cursor()
        cursor.executemany(sql, rows)
        self.push(rows)


class RowSQLAlchemyLoader(SQLAlchemyConnectionNode):
    def run(self, rows, conn, table, **kwargs):
        sql = get_bulk_replace(table, rows[0].keys(), dicts=False)
        conn.execute(sql, rows)
        self.push(rows)


class RowSQLiteTempLoader(SQLiteConnectionNode):
    def run(self, rows, conn, cursor=None, schema=None, **kwargs):
        assert isinstance(rows[0], sqlite3.Row), "Only sqlite3.Row rows are supported"
        table = get_temp_table(conn, rows, create=True, schema=schema)
        sql = get_bulk_replace(
            table.name, rows[0].keys(), dicts=False, value_string="?"
        )
        if not cursor:
            cursor = conn.cursor()
        cursor.executemany(sql, rows)
        self.push([table.name])


class RowSQLLoader(SQLConnectionNode):
    """Generic SQL Loader"""

    def run(self, rows, conn, table, cursor=None, **kwargs):
        sql = self.get_bulk_replace(conn, table, rows)
        if not cursor:
            cursor = self.get_sql_executor(conn)
        self.sql_executemany(conn, cursor, sql, rows)
        self.push(rows)


class RowSQLTempLoader(SQLConnectionNode):
    def run(self, rows, conn, cursor=None, schema=None, **kwargs):
        table = get_temp_table(conn, rows, create=True, schema=schema)
        sql = self.get_bulk_replace(conn, table.name, rows)
        if not cursor:
            cursor = self.get_sql_executor(conn)
        self.sql_executemany(conn, cursor, sql, rows)
        self.push([table.name])
