import random
import time

import pandas as pd
from pandas.io.sql import (
    SQLTable,
    SQLDatabase,
    SQLiteTable,
    SQLiteDatabase,
    pandasSQL_builder,
)
import sqlalchemy as sa
import sqlite3

SQLALCHEMY_CONN_TYPES = [sa.engine.base.Connection, sa.engine.Connectable]


def is_sqlalchemy_conn(conn):
    return isinstance(conn, tuple(SQLALCHEMY_CONN_TYPES))


def get_temp_table_name():
    return "temp_%s_%s" % (int(time.time()), random.randint(0, 1e6))


class TemporaryTable(SQLTable):
    def _execute_create(self):
        self.table = self.table.tometadata(self.pd_sql.meta)
        self.table._prefixes.append("TEMPORARY")
        self.table.create()


class SQLiteTemporaryTable(SQLiteTable):
    def _execute_create(self):
        with self.pd_sql.run_transaction() as conn:
            for stmt in self.table:
                if "CREATE TABLE" in stmt:
                    stmt = stmt.replace("CREATE TABLE", "CREATE TEMP TABLE")
                conn.execute(stmt)


def get_temp_table(conn, data, create=False, **kwargs):
    table_name = get_temp_table_name()
    pandas_engine = pandasSQL_builder(conn, schema=kwargs.get("schema", None))
    if isinstance(conn, sqlite3.Connection):
        cls = SQLiteTemporaryTable
    elif is_sqlalchemy_conn(conn):
        cls = TemporaryTable
    else:
        assert False, "Only sqlite3 and SQLAlchemy conns are currently supported"

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
    table_name, column_names, dicts=True, value_string="%s", type="REPLACE"
):
    assert type.lower() in ("replace", "insert", "insert ignore"), (
        "Invalid statement type: %s" % type
    )
    columns_clause = ", ".join(["`%s`" % c for c in column_names])
    if dicts:
        values_clause = ", ".join(["%%(%s)s" % c for c in column_names])
    else:
        values_clause = ", ".join(["%s" % value_string for c in column_names])
    sql = "%s INTO %s (%s) VALUES (%s)" % (
        type,
        table_name,
        columns_clause,
        values_clause,
    )
    return sql


def get_bulk_insert(table_name, column_names, **kwargs):
    return get_bulk_statement(table_name, column_names, type="INSERT", **kwargs)


def get_bulk_insert_ignore(table_name, column_names, **kwargs):
    return get_bulk_statement(table_name, column_names, type="INSERT IGNORE", **kwargs)


def get_bulk_replace(table_name, column_names, **kwargs):
    return get_bulk_statement(table_name, column_names, type="REPLACE", **kwargs)


def build_table_select(table, where=None, limit=None):
    sql = "SELECT * FROM %s" % table

    if where:
        if isinstance(where, str):
            sql = sql + " " + where
        else:
            sql = sql + " WHERE 1 AND "
            sql += " AND ".join(where)
    if limit:
        sql += " LIMIT %s" % int(limit)

    return sql
