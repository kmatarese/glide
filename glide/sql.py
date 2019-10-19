import sqlite3

from glide.core import Node
from glide.flow import SkipFalseNode
from glide.sql_utils import is_sqlalchemy_conn, get_bulk_statement


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
        assert self.allowed_conn_types and isinstance(
            self.allowed_conn_types, (list, tuple)
        ), (
            "%s.allowed_conn_types must be a list or tuple of connection types"
            % self.__class__.__name__
        )
        super().__init__(*args, **kwargs)

    def begin(self):
        conn = self.context.get("conn", None) or self.global_state.get("conn", None)
        assert conn, (
            "%s requires a conn argument in context or global state"
            % self.__class__.__name__
        )
        self.check_conn(conn)

    def _is_allowed_conn(self, conn):
        return isinstance(conn, tuple(self.allowed_conn_types))

    def check_conn(self, conn):
        """Check the database connection"""
        assert self._is_allowed_conn(conn), (
            "Connection type %s is not in allowed types: %s"
            % (type(conn), self.allowed_conn_types)
        )

    def get_sql_executor(self, conn, cursor_type=None):
        """Get the object that can execute queries"""
        if is_sqlalchemy_conn(conn):
            return conn
        return conn.cursor(cursor_type) if cursor_type else conn.cursor()

    def sql_execute(self, conn, cursor, sql, params=None, **kwargs):
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

    def sql_executemany(self, conn, cursor, sql, rows):
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
        A SQL bulk replace query

        """
        if is_sqlalchemy_conn(conn):
            return get_bulk_statement(
                stmt_type, table, rows[0].keys(), dicts=False, odku=odku
            )

        if isinstance(conn, sqlite3.Connection):
            assert isinstance(
                rows[0], sqlite3.Row
            ), "Only sqlite3.Row rows are supported"
            return get_bulk_statement(
                stmt_type,
                table,
                rows[0].keys(),
                dicts=False,
                value_string="?",
                odku=odku,
            )

        assert not isinstance(
            rows[0], tuple
        ), "Dict rows expected, got tuple. Please use a dict cursor."
        return get_bulk_statement(stmt_type, table, rows[0].keys(), odku=odku)


class SQLNode(BaseSQLNode, SQLCursorPushMixin):
    """A generic SQL node that will behave differently based on the connection type"""

    allowed_conn_types = [object]

    def check_conn(self, conn):
        """Make sure the object is a valid SQL connection"""
        assert hasattr(conn, "cursor") or is_sqlalchemy_conn(conn), (
            "Connection must have a cursor() method or be a SQLAlchemy connection: %s"
            % conn
        )


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
        fetcher = self.sql_execute(conn, cursor, sql, params=params, **kwargs)
        result = fetcher.fetchone()

        if isinstance(conn, sqlite3.Connection):
            assert isinstance(
                result, sqlite3.Row
            ), "Only sqlite3.Row rows are supported for sqlite3 connections"

        assert not isinstance(
            result, tuple
        ), "Dict rows expected, got tuple. Please use a dict cursor."

        assert "assert" in result.keys(), "Result is missing 'assert' column"
        result = result["assert"]

        if data_check:
            check = data_check(self, data)
            assert result == check, (
                "SQL assertion failed\nnode: %s\nsql: %s\nvalue: %s\ndata_check: %s"
                % (self.name, sql, result, check)
            )
        else:
            assert result, "SQL assertion failed\nnode: %s\nsql: %s\nvalue: %s" % (
                self.name,
                sql,
                result,
            )

        self.push(data)
