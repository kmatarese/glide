Creating Nodes
==============

To create a custom node you simply inherit from the Glide ``Node`` class and
define a ``run`` method that takes at least one positional argument for the data
being pushed to it. The ``run`` method should call ``self.push(data)`` with the
data it wants to push downstream.

Here is an example of a simple transformer node::

    class ExampleTransformer(Node):
        def run(self, data):
            # Do something to the data here
            self.push(data)


Node Context
============

Each node has a ``context``. This comes into play when ``run`` is called on
the node, as the required and optional parts of the context are inferred from
the positional and keyword args of ``run``. Take for example::

    class MyNode(Node):
        def run(self, data, conn, chunksize=None, **kwargs):
            # Some node-specific code here
            self.push(data)

By default all nodes expect their first positional arg to be the data going
through the pipeline. This node also requires a ``conn`` argument, and has an
optional ``chunksize`` argument. These values can be filled in from the
following inputs in priority order, with earlier methods overriding those
further down the list:

1. Context args passed to ``consume`` for the particular node:
   ::
        conn = get_my_db_conn()
        glider.consume(
            data,
            my_node=dict(conn=conn, chunksize=100)
        )

2. Default context set on the node at init time:
   ::
        conn = get_my_db_conn()
        glider = Glider(
            MyNode("my_node", conn=conn, chunksize=100)
        )

3. Global pipeline state passed via ``global_state``. This only works for populating positional args currently:
   ::
        conn = get_my_db_conn()
        glider = Glider(
            MyNode("my_node"),
            global_state=dict(conn=conn)
        )

Additionally, you can update the context of nodes at runtime by using the
``update_context`` or ``update_downstream_context`` node methods.

Runtime Context
===============

Sometimes it is useful or necessary to fill in node context values at
runtime. A prime example is when using SQL-based nodes in a parallel
processing context. Since the database connection objects can not be pickled
and passed to the spawned processes you need to establish the connection
within the subprocess. Glide has a special ``RuntimeContext`` class for this
purpose. Any callable wrapped as a RuntimeContext will not be called until
``consume`` is called. In the example below, ``get_pymysql_conn`` will be executed
in a subprocess to fill in the "conn" context variable for the "extract" node::

    glider = ProcessPoolParaGlider(
        SQLExtract("extract")
        | PrettyPrint("load")
    )
    glider.consume(
        [sql],
        extract=dict(
            conn=RuntimeContext(get_pymysql_conn),
            cursor_type=pymysql.cursors.DictCursor,
        )
    )

In this case it is also necessary to specify the cursor_type so ``SQLExtract``
can create a dict-based cursor for query execution within the subprocess as
required by ``SQLExtract``. Any args/kwargs passed to RuntimeContext will be
passed to the function when called.


Config Context
==============

``ConfigContext`` is an alternative type of ``RuntimeContext`` that can read a
config file to fill in node context. It supports reading from JSON, INI, or
YAML config files and optionally extracting specific data from the file. The
following shows an example of reading a key ("nrows") from a JSON structure::

    glider = Glider(
        CSVExtract("extract", nrows=ConfigContext("myconfig.json", key="nrows"))
        | Print("load")
    )
    glider.consume(...)

As another example, the following reads from an INI file and also passes a
callable for the ``key`` parameter to extract a value from the config::

    glider = Glider(
        CSVExtract("extract", nrows=ConfigContext(
            "myconfig.ini", key=lambda x: int(x["Section"]["nrows"])
        ))
        | Print("load")
    )
    glider.consume(...)

If no value is specified for ``key``, the entire config file is
returned. ``ConfigContext`` may be particularly useful when you want to load
sensitive information such as API login details that you would not want to
store in your code.


Cleaning Up
===========

Sometimes it is also necessary to call clean up functionality after processing
is complete. Sticking with the example above that utilizes SQL-based nodes in
a parallel processing context, you'll want to explicitly close your database
connections in each subprocess. The ``consume`` method accepts a ``cleanup``
argument that is a dictionary mapping argument names to cleaner functions. The
following example tells the ``Glider`` to call the function ``closer`` with the
value from ``extract_conn`` once ``consume`` is finished. Note that ``closer`` is a
convenience function provided by Glide that just calls ``close`` on the given
object.::

    glider = ProcessPoolParaGlider(
        SQLExtract("extract")
        | PrettyPrint("load")
    )
    glider.consume(
        [sql],
        cleanup=dict(extract_conn=closer),
        extract=dict(
            conn=RuntimeContext(get_pymysql_conn),
            cursor_type=pymysql.cursors.DictCursor,
        )
    )

The keys of the ``cleanup`` dict can either be explicit (node name prefixed) or
more generic arg names that will map that function to every node that has that
arg in its ``run`` method signature (so just "conn=" would have worked
too). It's often better to be explicit as shown here.

> **Note:** In single-process cases the use of ``cleanup`` is usually not
necessary, as you often have access to the objects you need to clean up in the
main process and can just do normal clean up there with context managers or
explicit calls to ``close`` methods.


Common Nodes
============

``Glide`` comes with a suite of nodes to handle common data processing
tasks. The easiest way to view the options and understand their behavior is to
peruse the module documentation and/or review the source code for each node.

- For extractor nodes, such as SQL/CSV/Excel/File/URL extractors, see :py:mod:`glide.extract`.
- For transformer nodes, see :py:mod:`glide.transform`.
- For filter nodes, see :py:mod:`glide.filter`.
- For loader nodes, such as SQL/CSV/Excel/File/URL loaders, see :py:mod:`glide.load`.
- For some additional flow control nodes see :py:mod:`glide.core`.

Keep in mind it's very easy to write your own nodes. If you don't see
something you want, or you want slightly different behavior, create your own
node. If you think it's something that could benefit other users please
contribute!
