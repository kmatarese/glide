import sqlite3

from tlbx import st

from glide.core import Node
from glide.flow import SkipFalseNode
from glide.sql_utils import (
    is_sqlalchemy_conn,
    is_sqlalchemy_transaction,
    get_bulk_statement,
    escape_string,
)
from glide.utils import dbg, raiseif, raiseifnot


class SQLCursorPushMixin:
    """Shared logic for SQL cursor-based nodes"""

    def do_push(self, cursor, chunksize=None):
        """Fetch data and push to the next node, obeying chunksize if passed

        Parameters
        ----------
        cursor
            A cursor-like object with fetchmany and fetchall methods
        chunksize : int, optional
            If truthy the data will be fetched and pushed in chunks
        """
        if chunksize:
            while True:
                chunk = cursor.fetchmany(chunksize)
                if not chunk:
                    break
                self.push(chunk)
        else:
            data = cursor.fetchall()
            self.push(data)


class BaseSQLNode(SkipFalseNode):
    """Base class for SQL-based nodes, checks for valid connection types on init

    Attributes
    ----------
    allowed_conn_types : list or tuple
        A list or tuple of connection types that are allowed
    """

    allowed_conn_types = None

    def __init__(self, *args, **kwargs):
        raiseifnot(
            self.allowed_conn_types
            and isinstance(self.allowed_conn_types, (list, tuple)),
            (
                "%s.allowed_conn_types must be a list or tuple of connection types"
                % self.__class__.__name__
            ),
        )
        super().__init__(*args, **kwargs)

    def begin(self):
        conn = self.context.get("conn", None) or self.global_state.get("conn", None)
        raiseifnot(
            conn,
            "%s requires a conn argument in context or global state"
            % self.__class__.__name__,
        )
        self.check_conn(conn)

    def _is_allowed_conn(self, conn):
        return isinstance(conn, tuple(self.allowed_conn_types))

    def check_conn(self, conn):
        """Check the database connection"""
        raiseifnot(
            self._is_allowed_conn(conn),
            "Connection type %s is not in allowed types: %s"
            % (type(conn), self.allowed_conn_types),
        )

    def get_sql_executor(self, conn, cursor_type=None):
        """Get the object that can execute queries"""
        if is_sqlalchemy_conn(conn):
            return conn
        return conn.cursor(cursor_type) if cursor_type else conn.cursor()

    def transaction(self, conn, cursor=None):
        """Start a transaction. If conn is a SQLAlchemy conn return a
        reference to the transaction object, otherwise just return the conn
        which should have commit/rollback methods."""

        dbg("starting transaction: %s" % conn)
        if is_sqlalchemy_conn(conn):
            return conn.begin()

        # For SQLite and DBAPI connections we explicitly call begin.
        # https://docs.python.org/3/library/sqlite3.html#sqlite3-controlling-transactions
        if not cursor:
            cursor = self.get_sql_executor(conn)
        cursor.execute("BEGIN")
        return conn

    def commit(self, obj):
        """Commit any currently active transactions"""

        if hasattr(obj, "commit"):
            obj.commit()
        elif is_sqlalchemy_conn(obj):
            # Hack: I don't want to have to pass around the transaction
            # between nodes since that requirement is really specific to
            # SQLAlchemy, and SQLAlchemy doesn't seem to provide a standard
            # way of obtaining the current transaction, so this approach is
            # lifted from the SQLAlchemy internals.
            raiseifnot(
                hasattr(obj, "_Connection__transaction"),
                "Could not find transaction attribute on SQLAlchemy object: %s" % obj,
            )
            if getattr(obj, "_Connection__transaction", None):
                obj._Connection__transaction.commit()
            else:
                # SQLAlchemy connections autocommit by default, so we assume
                # that happened.
                pass
        else:
            raise AssertionError(
                "Could not determine how to commit with object: %s" % obj
            )

    def rollback(self, obj):
        """Rollback any currently active transactions"""

        dbg("rolling back transaction: %s" % obj)
        if hasattr(obj, "rollback"):
            obj.rollback()
        elif is_sqlalchemy_conn(obj):
            # See note above about this hack
            raiseifnot(
                hasattr(obj, "_Connection__transaction"),
                "Could not find transaction attribute on SQLAlchemy object: %s" % obj,
            )
            if getattr(obj, "_Connection__transaction", None):
                obj._Connection__transaction.rollback()
            else:
                raise AssertionError(
                    "Trying to rollback a transaction but the SQLAlchemy "
                    "conn was not in a transaction. It may have "
                    "autocommitted."
                )
        else:
            raise AssertionError(
                "Could not determine how to rollback with object: %s" % obj
            )

    def create_like(self, conn, cursor, table, like_table, drop=False):
        """Create a table like another table, optionally trying to drop
        `table` first"""
        table = escape_string(str(table).strip("`"))
        like_table = escape_string(str(like_table).strip("`"))

        if drop:
            drop_sql = "drop table if exists %s" % table
            dbg(drop_sql)
            self.execute(conn, cursor, drop_sql)

        if isinstance(conn, sqlite3.Connection):
            get_create_sql = (
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
            )
            qr = self.execute(conn, cursor, get_create_sql, params=(like_table,))
            row = qr.fetchone()
            raiseifnot(
                isinstance(row, sqlite3.Row), "Only sqlite3.Row rows are supported"
            )
            create_sql = row["sql"].replace(like_table, table)
        else:
            # Assume this syntax works with most other SQL databases
            create_sql = "create table %s like %s" % (table, like_table)

        dbg(create_sql)
        self.execute(conn, cursor, create_sql)

    def drop_table(self, conn, cursor, table):
        """Drop tables all day long"""
        drop_sql = escape_string("drop table %s" % table)
        dbg(drop_sql)
        self.execute(conn, cursor, drop_sql)

    def rename_tables(self, conn, cursor, renames):
        """Execute one or more table renames"""
        for t1, t2 in renames:
            sql = escape_string("ALTER TABLE %s RENAME TO %s" % (t1, t2))
            dbg(sql)
            self.execute(conn, cursor, sql)

    def execute(self, conn, cursor, sql, params=None, **kwargs):
        """Executes the sql statement and returns an object that can fetch results

        Parameters
        ----------
        conn
            A SQL database connection object
        cursor
            A SQL database cursor
        sql : str
            A sql query to execute
        params : tuple, optional
            A tuple of params to pass to the execute method of the conn or cursor
        **kwargs
            kwargs passed through to execute()

        Returns
        -------
        cursor
            cursor object that has executed but not fetched a query.
        """
        params = params or ()
        if is_sqlalchemy_conn(conn):
            qr = conn.execute(sql, *params, **kwargs)
            return qr
        qr = cursor.execute(sql, params, **kwargs)
        return cursor

    def executemany(self, conn, cursor, sql, rows):
        """Bulk executes the sql statement and returns an object that can fetch results

        Parameters
        ----------
        conn
            A SQL database connection object
        cursor
            A SQL database cursor
        sql : str
            A sql query to execute
        rows
            Rows of data to bulk execute

        Returns
        -------
        cursor
            cursor object that has executed but not fetched a query.
        """
        if is_sqlalchemy_conn(conn):
            qr = conn.execute(sql, rows)
            return qr
        qr = cursor.executemany(sql, rows)
        return cursor

    def get_bulk_statement(self, conn, stmt_type, table, rows, odku=False):
        """Get a bulk execution SQL statement

        Parameters
        ----------
        conn
            A SQL database connection object
        stmt_type : str
            Type of SQL statement to use (REPLACE, INSERT, etc.)
        table : str
            name of a SQL table
        rows
            An iterable of dict rows. The first row is used to determine
            column names.
        odku : bool or list, optional
            If true, add ON DUPLICATE KEY UPDATE clause for all columns. If a
            list then only add it for the specified columns. **Note:** Backend
            support for this varies.

        Returns
        -------
        A SQL bulk load query of the given stmt_type

        """
        if is_sqlalchemy_conn(conn):
            return get_bulk_statement(
                stmt_type, table, rows[0].keys(), dicts=False, odku=odku
            )

        if isinstance(conn, sqlite3.Connection):
            raiseifnot(
                isinstance(rows[0], sqlite3.Row), "Only sqlite3.Row rows are supported"
            )
            return get_bulk_statement(
                stmt_type,
                table,
                rows[0].keys(),
                dicts=False,
                value_string="?",
                odku=odku,
            )

        raiseif(
            isinstance(rows[0], tuple),
            "Dict rows expected, got tuple. Please use a dict cursor.",
        )
        return get_bulk_statement(stmt_type, table, rows[0].keys(), odku=odku)


class SQLNode(BaseSQLNode, SQLCursorPushMixin):
    """A generic SQL node that will behave differently based on the connection type"""

    allowed_conn_types = [object]

    def check_conn(self, conn):
        """Make sure the object is a valid SQL connection"""
        raiseifnot(
            hasattr(conn, "cursor") or is_sqlalchemy_conn(conn),
            "Connection must have a cursor() method or be a SQLAlchemy connection: %s"
            % conn,
        )


class SQLExecute(SQLNode):
    def run(
        self,
        sql,
        conn,
        cursor=None,
        cursor_type=None,
        params=None,
        commit=True,
        rollback=False,
        dry_run=False,
        **kwargs
    ):
        """Perform a generic SQL query execution and push the cursor/execute
        response.

        Parameters
        ----------
        sql : str
            SQL query to run
        conn
            SQL connection object
        cursor : optional
            SQL connection cursor object
        cursor_type : optional
            SQL connection cursor type when creating a cursor is necessary
        params : tuple or dict, optional
            A tuple or dict of params to pass to the execute method
        commit : bool, optional
            If true try to commit the transaction. If your connection
            autocommits this will have no effect. If this is a SQLAlchemy
            connection and you are in a transaction, it will try to get a
            reference to the current transaction and call commit on that.
        rollback : bool, optional
            If true try to rollback the transaction on exceptions. Behavior
            may vary by backend DB library if you are not currently in a
            transaction.
        **kwargs
            Keyword arguments pushed to the execute method

        """
        fetcher = None

        if dry_run:
            warn(
                "dry_run=True, skipping sql execute in %s.run" % self.__class__.__name__
            )
        else:
            if not cursor:
                cursor = self.get_sql_executor(conn, cursor_type=cursor_type)

            params = params or ()

            try:
                fetcher = self.execute(conn, cursor, sql, params=params, **kwargs)
                if commit:
                    self.commit(conn)
            except:
                if rollback:
                    self.rollback(conn)
                raise

        self.push(fetcher)


class SQLFetch(Node, SQLCursorPushMixin):
    def run(self, cursor, chunksize=None):
        """Fetch data from the cursor and push the result.

        Parameters
        ----------
        cursor
            A cursor-like object that can fetch results
        chunksize : int, optional
            Fetch and push data in chunks of this size

        """
        self.do_push(cursor, chunksize=chunksize)


class SQLTransaction(SQLNode):
    def run(self, data, conn, cursor=None):
        """Begin a SQL transaction on the connection

        Parameters
        ----------
        data
            Data being passed through the pipeline
        conn
            Database connection to start the transaction on
        cursor : optional
            SQL connection cursor object

        """
        tx = self.transaction(conn, cursor=cursor)
        self.push(data)


class AssertSQL(SQLNode):
    def run(
        self,
        data,
        sql,
        conn,
        cursor=None,
        cursor_type=None,
        params=None,
        data_check=None,
        **kwargs
    ):
        """Run a SQL query to check data.

        Parameters
        ----------
        data
            Data to pass through on success
        sql : str
            SQL query to run. Should return a single row with a "assert"
            column to indicate success. Truthy values for "assert" will be
            considered successful, unless data_check is passed in which case
            it will be compared for equality to the result of that callable.
        conn
            SQL connection object
        cursor : optional
            SQL connection cursor object
        cursor_type : optional
            SQL connection cursor type when creating a cursor is necessary
        params : tuple or dict, optional
            A tuple or dict of params to pass to the execute method
        data_check : callable, optional
            A callable that will be passed the node and data as arguments and is expected
            to return a value to be compared to the SQL result.
        **kwargs
            Keyword arguments pushed to the execute method

        """
        if not cursor:
            cursor = self.get_sql_executor(conn, cursor_type=cursor_type)
        params = params or ()
        fetcher = self.execute(conn, cursor, sql, params=params, **kwargs)
        result = fetcher.fetchone()

        if isinstance(conn, sqlite3.Connection):
            raiseifnot(
                isinstance(result, sqlite3.Row),
                "Only sqlite3.Row rows are supported for sqlite3 connections",
            )

        raiseif(
            isinstance(result, tuple),
            "Dict rows expected, got tuple. Please use a dict cursor.",
        )

        raiseifnot("assert" in result.keys(), "Result is missing 'assert' column")
        result = result["assert"]

        if data_check:
            check = data_check(self, data)
            raiseifnot(
                result == check,
                (
                    "SQL assertion failed\nnode: %s\nsql: %s\nvalue: %s\ndata_check: %s"
                    % (self.name, sql, result, check)
                ),
            )
        else:
            raiseifnot(
                result,
                (
                    "SQL assertion failed\nnode: %s\nsql: %s\nvalue: %s"
                    % (self.name, sql, result)
                ),
            )

        self.push(data)
