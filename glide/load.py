"""A home for common data load nodes"""

from copy import deepcopy
import csv
from email.message import EmailMessage
import shutil
import sqlite3
import tempfile

import requests
from tlbx import st, pp, create_email, send_email, sqlformat, repr, format_msg

from glide.core import Node
from glide.flow import SkipFalseNode
from glide.sql import SQLNode
from glide.sql_utils import get_temp_table, add_table_suffix, escape_string
from glide.utils import (
    dbg,
    warn,
    raiseif,
    raiseifnot,
    size,
    save_excel,
    find_class_in_dict,
    get_class_list_docstring,
    open_filepath_or_buffer,
)


class Print(Node):
    """Print the data"""

    def print(self, data):
        """Print the data"""
        print(data)

    def get_label(self):
        """Get a label for the print statement"""
        return "---- %s ----\n" % self.name

    def run(self, data, label=True):
        """Print the data with the printer function and push"""
        if label:
            print(self.get_label(), end="")
        self.print(data)
        self.push(data)


class PrettyPrint(Print):
    """Pretty-prints the data"""

    def print(self, data):
        pp(data)


class LenPrint(Print):
    """Prints the length of the data"""

    def get_label(self):
        return "%s: " % self.name

    def print(self, data):
        print("data length: %s" % size(data, "n/a"))


class ReprPrint(Print):
    """Prints the reprlib.repr of the data"""

    def print(self, data):
        print(repr(data))


class FormatPrint(Node):
    """Format and print the data"""

    def run(
        self,
        data,
        label=None,
        indent=None,
        color=None,
        autocolor=False,
        format_func="pf",
    ):
        """Format using tlx.format_msg, then print

        Parameters
        ----------
        data
            The data to print
        **kwargs
            Keyword arguments passed to tlx.format_msg

        """
        if label == "node":
            label = self.name

        msg = format_msg(
            data,
            label=label,
            indent=indent,
            color=color,
            autocolor=autocolor,
            format_func=format_func,
        )
        print(msg)
        self.push(data)


class CSVLoad(SkipFalseNode):
    """Load data into a CSV using DictWriter"""

    def begin(self):
        """Initialize state for CSV writing"""
        self.writer = None

    def run(self, rows, f, push_file=False, dry_run=False, **kwargs):
        """Use DictWriter to output dict rows to a CSV.

        Parameters
        ----------
        rows
            Iterable of rows to load to a CSV
        f : file or buffer
            File to write rows to
        push_file : bool, optional
            If true, push the file forward instead of the data
        dry_run : bool, optional
            If true, skip actually loading the data
        **kwargs
            Keyword arguments passed to csv.DictWriter

        """
        close = False
        fo = f
        if isinstance(f, str):
            fo = open(f, "w")
            close = True

        try:
            if dry_run:
                warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
            else:
                if not self.writer:
                    if not kwargs.get("fieldnames", None):
                        try:
                            kwargs["fieldnames"] = rows[0].keys()
                        except TypeError as e:
                            raise TypeError(
                                "Unable to determine fieldnames from rows. "
                                "Either specify fieldnames or pass subscriptable data rows with keys()."
                            )
                    self.writer = csv.DictWriter(fo, **kwargs)
                    self.writer.writeheader()
                self.writer.writerows(rows)
        finally:
            if close:
                fo.close()

        if push_file:
            self.push(f)
        else:
            self.push(rows)

    def end(self):
        """Reset state in case the node gets reused"""
        self.writer = None


class ExcelLoad(SkipFalseNode):
    """Load data into an Excel file using pyexcel"""

    def run(
        self,
        rows,
        f,
        dict_rows=False,
        sheet_name="Sheet1",
        push_file=False,
        dry_run=False,
        **kwargs
    ):
        """Use DictWriter to output dict rows to a CSV.

        Parameters
        ----------
        rows
            Iterable of rows to load to an Excel file, or a dict of
            sheet_name->iterable for multi-sheet loads.
        f : file or buffer
            File to write rows to
        dict_rows : bool, optional
            If true the rows of each sheet will be converted from dicts to lists
        sheet_name : str, optional
            Sheet name to use if input is an iterable of rows. Unused otherwise.
        push_file : bool, optional
            If true, push the file forward instead of the data
        dry_run : bool, optional
            If true, skip actually loading the data
        **kwargs
            Keyword arguments passed to pyexcel

        """
        data = rows
        if not isinstance(rows, dict):
            # Setup as a single sheet
            data = {sheet_name: rows}

        if dict_rows:
            for _sheet_name, sheet_data in data.items():
                header = [list(sheet_data[0].keys())]
                data[_sheet_name] = header + [list(x.values()) for x in sheet_data]

        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            save_excel(f, data, **kwargs)

        if push_file:
            self.push(f)
        else:
            self.push(rows)


class SQLLoad(SQLNode):
    """Generic SQL loader"""

    def run(
        self,
        rows,
        conn,
        table,
        cursor=None,
        commit=True,
        rollback=False,
        stmt_type="REPLACE",
        odku=False,
        swap=False,
        keep_old=False,
        push_data=False,
        dry_run=False,
    ):
        """Form SQL statement and use bulk execute to write rows to table

        Parameters
        ----------
        rows
            Iterable of rows to load to the table
        conn
            Database connection
        table : str
            Name of a table to write the data to
        cursor : optional
            Database connection cursor
        commit : bool, optional
            If true try to commit the transaction. If your connection
            autocommits this will have no effect. If this is a SQLAlchemy
            connection and you are in a transaction, it will try to get a
            reference to the current transaction and call commit on that.
        rollback : bool, optional
            If true try to rollback the transaction on exceptions. Behavior
            may vary by backend DB library if you are not currently in a
            transaction.
        stmt_type : str, optional
            Type of SQL statement to use (REPLACE, INSERT, etc.). **Note:** Backend
            support for this varies.
        odku : bool or list, optional
            If true, add ON DUPLICATE KEY UPDATE clause for all columns. If a
            list then only add it for the specified columns. **Note:** Backend
            support for this varies.
        swap : bool, optional
            If true, load a table and then swap it into the target table via rename.
            Not supported with all database back ends.
        keep_old : bool, optional
            If true and swapping tables, keep the original table with a __old
            suffix added to the name
        push_data : bool, optional
            If true, push the data forward instead of the table name
        dry_run : bool, optional
            If true, skip actually loading the data

        """
        load_table = table
        if swap:
            load_table = add_table_suffix(table, "__swap")

        sql = self.get_bulk_statement(conn, stmt_type, load_table, rows, odku=odku)
        dbg("Loading %d rows\n%s" % (size(rows, "n/a"), sqlformat(sql)), indent="label")

        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            if not cursor:
                cursor = self.get_sql_executor(conn)

            try:
                if swap:
                    self.create_like(conn, cursor, load_table, table, drop=True)

                self.executemany(conn, cursor, sql, rows)

                if swap:
                    old_table = add_table_suffix(table, "__old")
                    self.rename_tables(
                        conn, cursor, [(table, old_table), (load_table, table)]
                    )
                    if not keep_old:
                        self.drop_table(conn, cursor, old_table)

                if commit:
                    self.commit(conn)
            except:
                if rollback:
                    self.rollback(conn)
                raise

        if push_data:
            self.push(rows)
        else:
            self.push(table)


class SQLTempLoad(SQLNode):
    """Generic SQL temp table loader"""

    def run(
        self,
        rows,
        conn,
        cursor=None,
        schema=None,
        commit=True,
        rollback=False,
        dry_run=False,
    ):
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
            If true try to commit the transaction. If your connection
            autocommits this will have no effect. If this is a SQLAlchemy
            connection and you are in a transaction, it will try to get a
            reference to the current transaction and call commit on that.
        rollback : bool, optional
            If true try to rollback the transaction on exceptions. Behavior
            may vary by backend DB library if you are not currently in a
            transaction.
        dry_run : bool, optional
            If true, skip actually loading the data

        """
        table = get_temp_table(conn, rows, create=True, schema=schema)
        sql = self.get_bulk_statement(conn, "REPLACE", table.name, rows)
        dbg("Loading %d rows\n%s" % (size(rows, "n/a"), sqlformat(sql)), indent="label")

        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            if not cursor:
                cursor = self.get_sql_executor(conn)

            try:
                self.executemany(conn, cursor, sql, rows)
                if commit:
                    self.commit(conn)
            except:
                if rollback:
                    self.rollback(conn)
                raise

        self.push(table.name)


class FileLoad(Node):
    """Load raw content to a file"""

    def run(self, data, f, open_flags="w", push_file=False, dry_run=False):
        """Load raw data to a file or buffer

        Parameters
        ----------
        data
            Data to write to file
        f : file path or buffer
            File path or buffer to write
        open_flags : str, optional
            Flags to pass to open() if f is not already an opened buffer
        push_file : bool
            If true, push the file forward instead of the data
        dry_run : bool, optional
            If true, skip actually loading the data

        """
        is_text = True
        if "b" in open_flags:
            is_text = False

        fo, _, close = open_filepath_or_buffer(
            f, open_flags=open_flags, is_text=is_text
        )

        try:
            if dry_run:
                warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
            else:
                fo.write(data)
        finally:
            if close:
                try:
                    fo.close()
                except ValueError:
                    pass

        if push_file:
            self.push(f)
        else:
            self.push(data)


class URLLoad(Node):
    """Load data to URL with requests"""

    def run(
        self,
        data,
        url,
        data_param="data",
        session=None,
        skip_raise=False,
        dry_run=False,
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
        skip_raise : bool, optional
            if False, raise exceptions for bad response status
        dry_run : bool, optional
            If true, skip actually loading the data
        **kwargs
            Keyword arguments to pass to the request method. If a dict is
            passed for the url parameter it overrides values here.

        """
        requestor = requests
        if session:
            requestor = session

        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            if isinstance(url, str):
                raiseif(
                    "data" in kwargs or "json" in kwargs,
                    "Overriding data/json params is not allowed",
                )
                kwargs[data_param] = data
                resp = requestor.post(url, **kwargs)
            elif isinstance(url, dict):
                kwargs_copy = deepcopy(kwargs)
                kwargs_copy.update(url)
                raiseif(
                    "data" in kwargs_copy or "json" in kwargs_copy,
                    "Overriding data/json params is not allowed",
                )
                kwargs_copy[data_param] = data
                resp = requestor.request(**kwargs_copy)
            else:
                raise AssertionError(
                    "Input url must be a str or dict type, got %s" % type(url)
                )

            if not skip_raise:
                resp.raise_for_status()

        self.push(data)


class EmailLoad(Node):
    """Load data to email via SMTP"""

    def run(
        self,
        data,
        frm=None,
        to=None,
        subject=None,
        body=None,
        html=None,
        attach_as="attachment",
        attachment_name=None,
        formatter=None,
        client=None,
        host=None,
        port=None,
        username=None,
        password=None,
        dry_run=False,
    ):
        """Load data to email via SMTP.

        Parameters
        ----------
        data
            EmailMessage or data to send. If the latter, the message will be
            created from the other node arguments.
        frm : str, optional
            The from email address
        to : str or list, optional
            A str or list of destination email addresses
        subject : str, optional
            The email subject
        body : str, optional
            The email text body
        html : str, optional
            The email html body
        attach_as : str
            Where to put the data in the email message if building the message
            from node arguments. Options: attachment, body, html.
        attachment_name: str, optional
            The file name to write the data to when attaching data to the
            email. The file extension will be used to infer the mimetype of
            the attachment. This should not be a full path as a temp directory
            will be created for this.
        formatter : callable
            A function to format and return a string from the input data if
            attach_as is set to "body" or "html".
        client : optional
            A connected smtplib.SMTP client
        host : str, optional
            The SMTP host to connect to if no client is provided
        port : int, optional
            The SMTP port to connect to if no client is provided
        username : str, optional
            The SMTP username for login if no client is provided
        password : str, optional
            The SMTP password for login if no client is provided
        dry_run : bool, optional
            If true, skip actually loading the data

        """

        if isinstance(data, EmailMessage):
            msg = data
        else:
            # Assume its data that needs to be converted to attachments and sent
            raiseifnot(
                frm and to and subject,
                "Node context must have frm/to/subject set to create an email msg",
            )
            raiseifnot(
                isinstance(data, str),
                "data must be passed as raw str content, got %s" % type(data),
            )

            attachments = None
            tmpdir = None

            if attach_as == "attachment":
                raiseifnot(
                    attachment_name,
                    "Must specify an attachment_name when attach_as = attachment",
                )
                tmpdir = tempfile.TemporaryDirectory()
                filename = tmpdir.name + "/" + attachment_name
                with open(filename, "w") as f:
                    f.write(data)
                attachments = [filename]
            else:
                fmt_data = formatter(data) if formatter else data
                if attach_as == "body":
                    body = (body or "") + fmt_data
                elif attach_as == "html":
                    html = (html or "") + fmt_data
                else:
                    raise AssertionError(
                        "Invalid attach_as value: %s, options: attachment, body, html"
                        % attach_as
                    )

            msg = create_email(
                frm, to, subject, body=body, html=html, attachments=attachments
            )

            if tmpdir:
                tmpdir.cleanup()

        if dry_run:
            warn("dry_run=True, skipping load in %s.run" % self.__class__.__name__)
        else:
            dbg("Sending msg %s to %s" % (msg["Subject"], msg["To"]))
            send_email(
                msg,
                client=client,
                host=host,
                port=port,
                username=username,
                password=password,
            )

        self.push(data)


node_names = find_class_in_dict(Node, locals(), include="Load")
node_names.extend(find_class_in_dict(Node, locals(), include="Print"))
if node_names:
    __doc__ = __doc__ + get_class_list_docstring("Nodes", node_names)
