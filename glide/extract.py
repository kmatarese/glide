"""A home for common data extraction nodes"""

import codecs
from collections import OrderedDict
from copy import deepcopy
import csv
from email import parser, policy
from io import BytesIO

from imapclient import IMAPClient
import requests
from tlbx import st, read_chunks, extract_email_payload

from glide.core import Node
from glide.sql_utils import build_table_select
from glide.sql import SQLNode
from glide.utils import (
    dbg,
    raiseif,
    raiseifnot,
    read_excel,
    find_class_in_dict,
    get_class_list_docstring,
    not_none,
    open_filepath_or_buffer,
)


class CSVExtract(Node):
    """Extract data from a CSV"""

    def run(
        self,
        f,
        compression=None,
        open_flags="r",
        chunksize=None,
        nrows=None,
        reader=csv.DictReader,
        **kwargs
    ):
        """Extract data for input file and push dict rows

        Parameters
        ----------
        f : file path or buffer
            file path or buffer to read CSV
        compression : str, optional
            param passed to pandas get_filepath_or_buffer
        open_flags : str, optional
            Flags to pass to open() if f is not already an opened buffer
        chunksize : int, optional
            Read data in chunks of this size
        nrows : int, optional
            Limit to reading this number of rows
        reader : csv Reader, optional
            The CSV reader class to use. Defaults to csv.DictReader
        **kwargs
            keyword arguments passed to the reader

        """
        if "b" in open_flags:
            raise Exception("Can not use binary open mode with CSVExtract")

        f, _, close = open_filepath_or_buffer(
            f, open_flags=open_flags, compression=compression
        )

        try:
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


class ExcelExtract(Node):
    """Extract data from an Excel file"""

    def run(self, f, dict_rows=False, **kwargs):
        """Use pyexcel to read data from a file

        Parameters
        ----------
        f : str or buffer
            The Excel file to read. Multiple excel formats supported.
        dict_rows : bool, optional
            If true the rows of each sheet will be converted to dicts with
            column names as keys.
        **kwargs
            Keyword arguments passed to pyexcel

        """
        data = read_excel(f, **kwargs)
        if dict_rows:
            for sheet_name in data.keys():
                data[sheet_name] = [
                    OrderedDict(zip(data[sheet_name][0], data[sheet_name][i]))
                    for i in range(1, len(data[sheet_name]))
                ]
        self.push(data)


class SQLExtract(SQLNode):
    """Generic SQL extract Node"""

    def run(
        self,
        sql,
        conn,
        cursor=None,
        cursor_type=None,
        params=None,
        chunksize=None,
        **kwargs
    ):
        """Extract data for input query and push fetched rows.

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
        chunksize : int, optional
            Fetch and push data in chunks of this size
        **kwargs
            Keyword arguments pushed to the execute method

        """
        if not cursor:
            cursor = self.get_sql_executor(conn, cursor_type=cursor_type)
        params = params or ()
        fetcher = self.execute(conn, cursor, sql, params=params, **kwargs)
        self.do_push(fetcher, chunksize=chunksize)


class SQLParamExtract(SQLExtract):
    """Generic SQL extract node that expects SQL params as data instead of a query"""

    def run(
        self, params, sql, conn, cursor=None, cursor_type=None, chunksize=None, **kwargs
    ):
        """Extract data for input params and push fetched rows.

        Parameters
        ----------
        params : tuple or dict
            A tuple or dict of params to pass to the execute method
        sql : str
            SQL query to run
        conn
            SQL connection object
        cursor : optional
            SQL connection cursor object
        cursor_type : optional
            SQL connection cursor type when creating a cursor is necessary
        chunksize : int, optional
            Fetch and push data in chunks of this size
        **kwargs
            Keyword arguments pushed to the execute method

        """
        super().run(
            sql,
            conn,
            cursor=cursor,
            cursor_type=cursor_type,
            params=params,
            chunksize=chunksize,
            **kwargs
        )


class SQLTableExtract(SQLNode):
    """Generic SQL table extract node"""

    def run(
        self,
        table,
        conn,
        cursor=None,
        cursor_type=None,
        where=None,
        limit=None,
        params=None,
        chunksize=None,
        **kwargs
    ):
        """Extract data for input table and push fetched rows

        Parameters
        ----------
        table : str
            SQL table name
        conn
            SQL connection object
        cursor : optional
            SQL connection cursor object
        cursor_type : optional
            SQL connection cursor type when creating a cursor is necessary
        where : str, optional
            SQL where clause
        limit : int, optional
            Limit to put in SQL limit clause
        params : tuple or dict, optional
            A tuple or dict of params to pass to the execute method
        chunksize : int, optional
            Fetch and push data in chunks of this size
        **kwargs
            Keyword arguments passed to cursor.execute

        """
        if not cursor:
            cursor = self.get_sql_executor(conn, cursor_type=cursor_type)
        sql = build_table_select(table, where=where, limit=limit)
        params = params or ()
        fetcher = self.execute(conn, cursor, sql, params=params, **kwargs)
        self.do_push(fetcher, chunksize=chunksize)


class FileExtract(Node):
    """Extract raw data from a file"""

    def run(
        self,
        f,
        compression=None,
        open_flags="r",
        chunksize=None,
        push_lines=False,
        limit=None,
    ):
        """Extract raw data from a file or buffer and push contents

        Parameters
        ----------
        f : file path or buffer
            File path or buffer to read
        compression : str, optional
            param passed to pandas get_filepath_or_buffer
        open_flags : str, optional
            Flags to pass to open() if f is not already an opened buffer
        chunksize : int, optional
            Push lines in chunks of this size
        push_lines : bool, optional
            Push each line as it's read instead of reading entire file and pushing
        limit : int, optional
            Limit to first N lines
        """
        raiseif(
            chunksize and push_lines
        ), "Only one of chunksize and push_lines may be specified"

        is_text = True
        if "b" in open_flags:
            is_text = False

        f, _, close = open_filepath_or_buffer(
            f, open_flags=open_flags, compression=compression, is_text=is_text
        )

        try:
            data = []
            count = 0

            for line in f:
                count += 1
                if push_lines:
                    self.push(line)
                else:
                    data.append(line)
                    if chunksize and (count % chunksize == 0):
                        if is_text:
                            self.push("".join(data))
                        else:
                            self.push(b"".join(data))
                        data = []

                if limit and count >= limit:
                    break

            if ((not push_lines) and data) or count == 0:
                if is_text:
                    self.push("".join(data))
                else:
                    self.push(b"".join(data))

        finally:
            if close:
                try:
                    f.close()
                except ValueError:
                    pass


class URLExtract(Node):
    """Extract data from a URL with requests"""

    def run(
        self,
        request,
        data_type="content",
        session=None,
        skip_raise=False,
        page_size=None,
        page_size_param="size",
        page_offset_param="offset",
        page_request_param="params",
        page_key=None,
        page_len=len,
        page_limit=None,
        push_pages=False,
        **kwargs
    ):
        """Extract data from a URL using requests and push
        response.content. Input request may be a string (GET that url) or a
        dictionary of args to requests.request:

        http://2.python-requests.org/en/master/api/?highlight=get#requests.request

        See the requests docs for information on authentication options:

        https://requests.kennethreitz.org/en/master/user/authentication/

        Parameters
        ----------
        request : str or dict
            If str, a URL to GET. If a dict, args to requests.request
        data_type : str, optional
            One of "content", "text", or "json" to control extraction of
            data from requests response.
        session : optional
            A requests Session to use to make the request
        skip_raise : bool, optional
            if False, raise exceptions for bad response status
        page_size : int, optional
            If specified, request in pages of this size. Only supported with
            data_type="json".
        page_size_param : str, optional
            The request parameter to put the page size in
        page_offset_param : str, optional
            The request parameter to put the page offset in
        page_request_param : str, optional
            Where to put the paging params when calling requests. Can either be
            "params" or "data".
        page_key : str or callable, optional
            Where to pull the page data from the results. If None, assume the
            entire json response is the page data.
        page_len : callable
            A callable that can determine the length of the page given the
            json result. The default is just to use len(result).
        page_limit : int, optional
            If passed, use as a cap of the number of pages pulled
        push_pages : bool, optional
            If true, push each page individually.
        **kwargs
            Keyword arguments to pass to the request method. If a dict is
            passed for the request parameter it overrides values of kwargs.

        """
        requestor = requests
        if session:
            requestor = session

        paging = False
        if page_size or push_pages:
            paging = True
            raiseifnot(
                not_none(
                    page_request_param,
                    page_size,
                    push_pages,
                    page_size_param,
                    page_offset_param,
                ),
                "Not all paging params specified",
            )
            raiseifnot(
                page_request_param in ["data", "params"],
                "Invalid page_request_param: %s" % page_request_param,
            )
            raiseifnot(
                data_type == "json", "Paging is only supported with JSON-based results"
            )
            kwargs[page_request_param] = kwargs.get(page_request_param, {})

        offset = 0
        results = []

        if isinstance(request, str):
            request = dict(method="GET", url=request)
        else:
            raiseifnot(
                isinstance(request, dict),
                "Request must be a str or dict type, got %s" % type(request),
            )

        count = 0
        while True:
            kwargs_copy = deepcopy(kwargs)
            kwargs_copy.update(request)
            if paging:
                raiseif(
                    page_size_param in kwargs_copy[page_request_param]
                    or page_offset_param in kwargs_copy[page_request_param],
                    ("Params conflict with paging params: %s" % kwargs_copy),
                )
                kwargs_copy[page_request_param].update(
                    {page_size_param: page_size, page_offset_param: offset}
                )

            resp = requestor.request(**kwargs_copy)
            count += 1

            if not skip_raise:
                resp.raise_for_status()

            if data_type == "content":
                data = resp.content
            elif data_type == "text":
                data = resp.text
            elif data_type == "json":
                data = resp.json()
            else:
                raise AssertionError(
                    "Unrecognized data_type: %s, must be one of content, text, or json"
                    % data_type
                )

            if paging:
                page = data
                if page_key:
                    if isinstance(page_key, str):
                        page = data[page_key]
                    else:
                        page = page_key(data)

                offset += page_len(page)
                if push_pages:
                    self.push(page)
                else:
                    results.extend(page)

                if page_limit and count >= page_limit:
                    break
            else:
                results = data
                break

        if (not paging) or (not push_pages):
            self.push(results)


class EmailExtract(Node):
    """Extract data from an email inbox using IMAPClient: https://imapclient.readthedocs.io"""

    def run(
        self,
        criteria,
        sort=None,
        folder="INBOX",
        client=None,
        host=None,
        username=None,
        password=None,
        push_all=False,
        push_type="message",
        limit=None,
        **kwargs
    ):
        """Extract data from an email inbox and push the data forward.

        Note
        ----
        Instances of IMAPClient are NOT thread safe. They should not be shared
        and accessed concurrently from multiple threads.

        Parameters
        ----------
        criteria : str or list
            Criteria argument passed to IMAPClient.search. See
            https://tools.ietf.org/html/rfc3501.html#section-6.4.4.
        sort : str or list, optional
            Sort criteria passed to IMAPClient.sort. Note that SORT is an
            extension to the IMAP4 standard so it may not be supported by all
            IMAP servers. See https://tools.ietf.org/html/rfc5256.
        folder : str, optional
            Folder to read emails from
        client : optional
            An established IMAPClient connection. If not present, the
            host/login information is required.
        host : str, optional
            The IMAP host to connect to
        username : str, optional
            The IMAP username for login
        password : str, optional
            The IMAP password for login
        push_all : bool, optional
            When true push all retrievd data/emails at once
        push_type : str, optional
            What type of data to extract and push from the emails. Options include:

                * **message**: push email.message.EmailMessage objects
                * **message_id**: push a list of message IDs that can be fetched
                * **all**: push a list of dict(message=<email.message.EmailMessages>, payload=<extracted payload>)
                * **body**: push a list of email bodies
                * **attachment**: push a list of attachments (an email with multiple attachments will be grouped in a sublist)
        limit : int, optional
            Limit to N rows
        **kwargs
            Keyword arguments to pass IMAPClient if not client is passed

        """
        data = []
        logout = False
        push_types = ["message_id", "message", "all", "body", "attachment"]

        if not client:
            raiseifnot(
                host and username and password,
                "Host/Username/Password required to create IMAPClient",
            )
            dbg("Logging into IMAPClient %s/%s" % (host, username))
            logout = True
            client = IMAPClient(host, **kwargs)
            client.login(username, password)

        try:
            client.select_folder(folder)
            if sort:
                messages = client.sort(sort, criteria=criteria)
            else:
                messages = client.search(criteria)
            dbg("Found %d email messages" % len(messages))

            if push_type == "message_id":
                if limit:
                    data = messages[:limit]
                else:
                    data = messages
            else:
                raiseifnot(
                    push_type in push_types,
                    "Unrecognized push_type: %s, options: %s" % (push_type, push_types),
                )
                count = 0
                for msg_id, msg_data in client.fetch(messages, ["RFC822"]).items():
                    raw = msg_data[b"RFC822"].decode("utf8")
                    msg = parser.Parser(policy=policy.default).parsestr(raw)

                    if push_type == "message":
                        data.append(msg)
                    else:
                        payload = extract_email_payload(msg)
                        if push_type == "body":
                            data.append(payload[0])
                        elif push_type == "attachment":
                            data.append(payload[1:])
                        elif push_type == "all":
                            data.append(dict(message=msg, payload=payload))

                    count += 1
                    if limit and count >= limit:
                        break

        finally:
            if logout:
                client.logout()

        if push_all:
            self.push(data)
        else:
            for row in data:
                self.push(row)


node_names = find_class_in_dict(Node, locals(), include="Extract")
if node_names:
    __doc__ = __doc__ + get_class_list_docstring("Nodes", node_names)
