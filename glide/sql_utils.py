"""SQL utilities"""

import random
import sqlite3
import time

import pandas as pd
from pandas.io.sql import SQLTable, SQLiteTable, pandasSQL_builder
from pymysql import escape_string
import sqlalchemy as sa
from tlbx import st

SQLALCHEMY_CONN_TYPES = [sa.engine.base.Connection, sa.engine.Connectable]


def is_sqlalchemy_conn(conn):
    """Check if conn is a sqlalchemy connection"""
    return isinstance(conn, tuple(SQLALCHEMY_CONN_TYPES))


def is_sqlalchemy_transaction(o):
    """Check if an object is a sqlalchemy transaction"""
    return isinstance(conn, sa.engine.Transaction)


def add_table_suffix(table, suffix):
    """Helper to deal with backticks when adding table suffix"""
    table = str(table)  # Hack to handle SQLAlchemy tables
    if table.endswith("`"):
        table = table.rstrip("`") + suffix + "`"
    else:
        table = table + suffix
    return table


def get_temp_table_name():
    """Create a unique temp table name"""
    return "temp_%s_%s" % (int(time.time()), random.randint(0, 1e6))


class TemporaryTable(SQLTable):
    """Override the default Pandas table creation to make it a temp table"""

    def _execute_create(self):
        """Inject TEMPORARY keyword into create statement"""
        self.table = self.table.tometadata(self.pd_sql.meta)
        self.table._prefixes.append("TEMPORARY")
        self.table.create()


class SQLiteTemporaryTable(SQLiteTable):
    """Override the default Pandas SQLite table creation to make it a temp table"""

    def _execute_create(self):
        """Inject TEMP keyword into create statement"""
        with self.pd_sql.run_transaction() as conn:
            for stmt in self.table:
                if "CREATE TABLE" in stmt:
                    stmt = stmt.replace("CREATE TABLE", "CREATE TEMP TABLE")
                conn.execute(stmt)


def get_temp_table(conn, data, create=False, **kwargs):
    """Reuse Pandas logic for creating a temp table. The definition will be
    formed based on the first row of data passed"""

    table_name = get_temp_table_name()
    pandas_engine = pandasSQL_builder(conn, schema=kwargs.get("schema", None))
    if isinstance(conn, sqlite3.Connection):
        cls = SQLiteTemporaryTable
    elif is_sqlalchemy_conn(conn):
        cls = TemporaryTable
    else:
        raise AssertionError(
            "Only sqlite3 and SQLAlchemy conns are currently supported"
        )

    df = data
    if not isinstance(data, pd.DataFrame):
        # This reuses pandas logic for creating tables, but doesn't fully
        # convert rows to a dataframe since that may be expensive and
        # unwanted. Assumes its an iterable of dicts and keys() returns column
        # names in proper order.
        first_row = data[0]
        df = pd.DataFrame.from_records([first_row], columns=first_row.keys())

    table = cls(table_name, pandas_engine, frame=df, if_exists="fail")
    if create:
        table.create()
    return table


def get_bulk_statement(
    stmt_type, table_name, column_names, dicts=True, value_string="%s", odku=False
):
    """Get a SQL statement suitable for use with bulk execute functions

    Parameters
    ----------
    stmt_type : str
        One of REPLACE, INSERT, or INSERT IGNORE. **Note:** Backend support for
        this varies.
    table_name : str
        Name of SQL table to use in statement
    column_names : list
        A list of column names to load
    dicts : bool, optional
        If true, assume the data will be a list of dict rows
    value_string : str, optional
        The parameter replacement string used by the underyling DB API
    odku : bool or list, optional
        If true, add ON DUPLICATE KEY UPDATE clause for all columns. If a list
        then only add it for the specified columns. **Note:** Backend support for
        this varies.

    Returns
    -------
    sql : str
        The sql query string to use with bulk execute functions

    """
    if not stmt_type.lower() in ("replace", "insert", "insert ignore"):
        raise AssertionError("Invalid statement type: %s" % stmt_type)
    columns_clause = ", ".join(["`%s`" % c for c in column_names])
    if dicts:
        values_clause = ", ".join(["%%(%s)s" % c for c in column_names])
    else:
        values_clause = ", ".join(["%s" % value_string for c in column_names])
    sql = "%s INTO %s (%s) VALUES (%s)" % (
        stmt_type,
        table_name,
        columns_clause,
        values_clause,
    )

    if odku:
        odku_cols = column_names
        if isinstance(odku, (list, tuple)):
            odku_cols = odku
        odku_clause = ", ".join(["%s=VALUES(%s)" % (col, col) for col in odku_cols])
        sql = sql + " ON DUPLICATE KEY UPDATE %s" % odku_clause

    return escape_string(sql)


def get_bulk_insert(table_name, column_names, **kwargs):
    """Get a bulk insert statement"""
    return get_bulk_statement("INSERT", table_name, column_names, **kwargs)


def get_bulk_insert_ignore(table_name, column_names, **kwargs):
    """Get a bulk insert ignore statement"""
    return get_bulk_statement("INSERT IGNORE", table_name, column_names, **kwargs)


def get_bulk_replace(table_name, column_names, **kwargs):
    """Get a bulk replace statement"""
    return get_bulk_statement("REPLACE", table_name, column_names, **kwargs)


def build_table_select(table, where=None, limit=None):
    """Simple helper to build a SQL query to select from a table"""
    sql = "SELECT * FROM %s" % table

    if where:
        if isinstance(where, str):
            sql = sql + " " + where
        else:
            sql = sql + " WHERE 1 AND "
            sql += " AND ".join(where)
    if limit:
        sql += " LIMIT %s" % int(limit)

    return escape_string(sql)
