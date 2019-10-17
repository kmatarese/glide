Glide: Easy ETL
===============

[![Generic badge](https://img.shields.io/badge/Status-Alpha-yellow.svg)](https://shields.io/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Documentation Status](https://readthedocs.org/projects/glide-etl/badge/?version=latest)](https://glide-etl.readthedocs.io/en/latest/?badge=latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Introduction
------------

Glide is an easy-to-use data pipelining tool inspired by [Consecution](https://github.com/robdmc/consecution) and 
[Apache Storm Topologies](http://storm.apache.org/releases/current/Tutorial.html). Like those libraries, **Glide is:**

- A simple, reusable approach to building robust ETL pipelines
- A system for wiring together processing nodes to form a directed acyclic graph (DAG)

**Glide also has:**

- An expanding suite of built-in nodes and pipelines that extract, transform, and load data from/to any combination of:
  - SQL databases (SQLite, DBAPI, and SQLAlchemy support)
  - URLs
  - Local or remote files including:
    - CSVs
    - Excel files (including multi-sheet support)
    - Raw/generic files
  - Emails
- Extensions for [Pandas](https://pandas.pydata.org/), [Dask](https://dask.org/), [Celery](http://www.celeryproject.org/), [Redis Queue](http://python-rq.org/) and more
- A variety of node and DAG parallel/distributed processing strategies including concurrent.futures Executors, Dask, Celery, or Redis Queue.
- A simple decorator to generate a command line interface from a pipeline in ~one line of code
- The ability to control node contexts via defaults and/or simple runtime overrides

**Glide is not** a task orchestration and/or dependency management tool like
Airflow. Use Glide to define your easily developed/contained/reusable/testable
data processing pipelines and then rely on a tool like Airflow to do what it's
good at, namely scheduling and complex task dependency management.

Table of Contents
-----------------

- [Installation](#installation)
- [Examples](#examples) 
- [Creating Nodes](#creatingnodes)
- [CLI Generation](#cligeneration)
- [Extensions](#extensions)
- [Docs](#documentation)
- [How to Contribute](#howtocontribute)

<a name="installation"></a>
Installation
------------

> ⚠️ **Warning**: This project is in an alpha state and is rapidly changing. It should not be used in production.

```shell
$ pip install glide
```

<a name="examples"></a>
Examples
--------

The following examples serve as a quickstart to illustrate some core features
and built-in nodes. More complete documentation can be viewed
[here](https://glide-etl.readthedocs.io/en/latest/index.html).

`Glider` is the main pipeline class that takes a DAG of `Nodes` as input and
then accepts data to process in its `consume` method. The examples below
assume you have used the following shortcut to import all necessary node and
pipeline classes:

```python
from glide import *
```

Let's build some pipelines to explore Glide further...

### Example: CSV Extract, Transform, Load

Here is a simple example applying a transformation to data from a CSV, in this
case using an existing function to lowercase all strings before loading into
an output CSV:

```python
def lower_rows(data):
    for row in data:
        for k, v in row.items():
            row[k] = v.lower() if type(v) == str else v
    return data

glider = Glider(
    CSVExtract("extract")
    | Func("transform", func=lower_rows)
    | CSVLoad("load")
)
glider.consume(
    ["/path/to/infile.csv"],
    extract=dict(chunksize=100),
    load=dict(outfile="/path/to/outfile.csv"),
)
```

### Node Context

The above examples demonstrates two separate ways to pass context to nodes:
        
1. Passing kwargs when instantiating the node. This becomes a default context
for the node any time it is used/reused.
2. Passing kwargs to `consume` that are node_name->node_context pairs. This context
lasts only for the `consume` call. 

> **Note:** Further details can be found in the Creating Nodes section.

### Example: Global State

A `Glider` can also have a shared context that can be used to populate node
arguments via its optional `global_state` argument:

```python
conn = get_my_sqlalchemy_conn()
sql = "select * from in_table limit 10"

glider = Glider(
    SQLExtract("extract")
    | SQLLoad("load"),
    global_state=dict(conn=conn) # conn will automagically be passed to any nodes that require it
)
glider.consume(
    [sql],
    load=dict(table="out_table")
)
```

### Example: Placeholder Nodes

You can easily drop replacement nodes into a pipeline:

```python
glider = Glider(
    PlaceholderNode("extract")
    | MyTransformer("transform")
    | CSVLoad("load")
)
glider["extract"] = CSVExtract("extract")
glider.consume(...)
```

This is one basic approach to make reusable, templated pipelines.

> **Note:** Any node can be replaced by name. `PlaceholderNode` is just a convenience.

### Example: Parallel Transformation

Let's redo the earlier example with the data transformed in parallel processes
using a `ProcessPoolSubmit` node. In this case, `lower_rows` will be called on
equal splits of the data read from the CSV:

```python
glider = Glider(
    CSVExtract("extract")
    | ProcessPoolSubmit("transform", push_type=PushTypes.Result)
    | CSVLoad("load")
)
glider.consume(
    ["infile.csv"],
    transform=dict(func=lower_rows),
    load=dict(outfile="outfile.csv"),
)
```

We passed `push_type=PushTypes.Result` to force `ProcessPoolSubmit` to fetch
and combine the asynchronous results before pushing to the downstream
node. The default is to just pass the asynchronous task/futures objects
forward, so the following would be equivalent:

```python
glider = Glider(
    CSVExtract("extract")
    | ProcessPoolSubmit("transform")
    | FuturesReduce("reduce")
    | Flatten("flatten")
    | CSVLoad("load")
)
```

The `FuturesReduce` node waits for the results from each futures object, and
then `Flatten` will combine each subresult back together into a single result
to be loaded in the final `CSVLoad` node.

### Example: Parallel Pipelines via ParaGlider

Glide also has support for completely parallelizing pipelines using a
`ParaGlider` (who said ETL isn't fun?!?) instead of a `Glider`. The following
code will create a `ProcessPoolParaGlider` and split processing of the inputs (two files
in this case) over the pool, with each process running the entire pipeline on
part of the consumed data:

```python
glider = ProcessPoolParaGlider(
    CSVExtract('extract')
    | Print('load')
)
glider.consume(
    ["/path/to/infile1.csv", "/path/to/infile2.csv"],
    extract=dict(nrows=50)
)
```

### Example: Parallel Branching

If you only want to execute part of the pipeline in parallel, you can branch
into parallel execution in the middle of the DAG utilizing a parallel push
node as in the following example:

```python
glider = Glider(
    CSVExtract("extract", nrows=60)
    | ProcessPoolPush("push", split=True)
    | [Print("load1"), Print("load2"), Print("load3")]
)
glider.consume(["/path/to/infile.csv"])
```

The above example will extract 60 rows from a CSV and then push equal slices
to the logging nodes in parallel processes. Using `split=False` (default)
would have passed the entire 60 rows to each logging node in parallel
processes. 

Once you branch off into processes with a parallel push node there is no way
to reduce/join the pipeline back into the original process and resume
single-process operation. The entire remainder of the pipeline is executed in
each subprocess. However, that can be achieved with threads if necessary as
shown in the next example.

### Example: Thread Reducers

```python
glider = Glider(
    CSVExtract("extract", nrows=60)
    | ThreadPoolPush("push", split=True)
    | [Print("load1"), Print("load2"), Print("load3")]
    | ThreadReduce("reduce")
    | Print("loadall")
)
glider.consume(["/path/to/infile.csv"])
```

The above code will split the data and push to the first 3 logging nodes in
multiple threads. The `ThreadReduce` node won't push until all of the previous
nodes have finished, and then the final logging node will print all of the
results.

### Summary of Parallel Processing

At this point it's worth summarizing the various ways you can attempt parallel processing
using Glide:

- Method 1: Parallelization *within* nodes such as `ProcessPoolSubmit` or a distributed processing extension such as Dask/Celery/Redis Queue
- Method 2: Completely parallel pipelines via `ParaGliders` (each process executes the entire pipeline)
- Method 3: Branched parallelism using parallel push nodes such as `ProcessPoolPush` or `ThreadPoolPush`

Each has its own use cases. Method 1 is perhaps the most straightforward since
you can return to single process operation after the node is done doing whatever
it needed to do in parallel. Method 2 may be useful and easy to understand in
certain cases as well. Method 3 can lead to more complex/confusing flows and
should probably only be used towards the end of pipelines to branch the output
in parallel, such as if writing to several databases in parallel as a final
step.

> **Note:** Combining the approaches may not work and has not been
tested. Standard limitations apply regarding what types of data can be
serialized and passed to a parallel process.

### Runtime Context Generation

Sometimes it is useful or necessary to fill in node context values at
runtime. A prime example is when using SQL-based nodes in a parallel
processing context. Since the database connection objects can not be pickled
and passed to the spawned processes you need to establish the connection
within the subprocess. Glide has a special `RuntimeContext` class for this
purpose. Any callable wrapped as a RuntimeContext will not be called until
`consume` is called. In the example below, `get_pymysql_conn` will be executed
in a subprocess to fill in the "conn" context variable for the "extract" node:

```python
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
```

In this case it is also necessary to specify the cursor_type so `SQLExtract`
can create a dict-based cursor for query execution within the subprocess as
required by `SQLExtract`. Any args/kwargs passed to RuntimeContext will be
passed to the function when called.

### Cleaning Up

Sometimes it is also necessary to call clean up functionality after processing
is complete. Sticking with the example above that utilizes SQL-based nodes in
a parallel processing context, you'll want to explicitly close your database
connections in each subprocess. The `consume` method accepts a `cleanup`
argument that is a dictionary mapping argument names to cleaner functions. The
following example tells the `Glider` to call the function `closer` with the
value from `extract_conn` once `consume` is finished. Note that `closer` is a
convenience function provided by Glide that just calls `close` on the given
object.

```python
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
```

The keys of the `cleanup` dict can either be explicit (node name prefixed) or
more generic arg names that will map that function to every node that has that
arg in its `run` method signature (so just "conn=" would have worked
too). It's often better to be explicit as shown here.

> **Note:** In single-process cases the use of `cleanup` is usually not
necessary, as you often have access to the objects you need to clean up in the
main process and can just do normal clean up there with context managers or
explicit calls to `close` methods.

### Data Integrity Checks

Glide provides some handy assertion nodes for data integrity checks in your
pipeline. For example, you can use `AssertFunc` to assert that some condition
of the data is met:

```python
glider = Glider(
    CSVExtract("extract", chunksize=10, nrows=20)
    | AssertFunc("length_check", func=lambda node, data: len(data) == 10)
    | CSVLoad("load") 
)
```

The `func` callable must accept two parameters, a reference to the node object
and the data passed into that node. Any truthy value returned will pass the
assertion test. 

Another common case is to do a check against some SQL data, perhaps in an aggregation
pipeline. You can use `AssertSQL` for that, as in the following example that simply
verifies the number of rows inserted.

```python
glider = Glider(
    SQLExtract("extract")
    | SQLLoad("load")
    | AssertSQL("sql_check")
)

sql = "select * from in_table limit 10"
assert_sql = "select (select count(*) as x from out_table) == 10 as assert"

glider.consume(
    [sql],
    extract=dict(conn=in_conn),
    load=dict(conn=out_conn, table="out_table"),
    sql_check=dict(conn=out_conn, sql=assert_sql)
)
```

This looks for a truthy value in the `assert` column of the result to pass the
assertion. You can also use the `data_check` option of `AssertSQL` to instead
have it do a comparison to the result of some function of the data. The
following example will compare the `assert` column value to the result of the
`data_check` for equality. In this case, it compares a count from the database
to the length of the input data.

```python
glider = ...

sql = "select * from in_table limit 10"
assert_sql = "select count(*) as assert from out_table"

glider.consume(
    [sql],
    extract=dict(conn=in_conn),
    load=dict(conn=out_conn, table="out_table", push_data=True),
    sql_check=dict(
        conn=out_conn,
        sql=assert_sql,
        data_check=lambda node, data: len(data)
    )
)
```

Note that we also added `push_data=True` to the `SQLLoad` node to have it push
the data instead of a table name.

### Debug Logging

To enable debug logging for Glide change the log level of the "glide" logger:

```python
import logging
logging.getLogger("glide").setLevel(logging.DEBUG)
```

Glide will then print debug information about data passed through your
pipeline. 

You can also pass `log=True` to the init method of any node to enable logging
of processed items:

```python
glider = Glider(
    SQLExtract("extract", log=True)
    ...
)
```

Finally, there are a variety of print nodes you can place in your pipeline for
general logging or debugging, such as `Print`, `PrettyPrint`, `LenPrint`,
`ReprPrint`, and `FormatPrint`. See the node documentation for more info.


### Plotting Pipeline DAGs

If you have the [Graphviz](http://www.graphviz.org/) package installed, you can generate
a plot of your pipelines by simply doing the following:

```python
glider = Glider(...)
glider.plot("/path/to/filename.png")
```

<a name="creatingnodes"></a>
Creating Nodes
--------------

To create a custom node you simply inherit from the Glide `Node` class and
define a `run` method that takes at least one positional argument for the data
being pushed to it. The `run` method should call `self.push(data)` with the
data it wants to push downstream.

Here is an example of a simple transformer node:

```python
class ExampleTransformer(Node):
    def run(self, data):
        # Do something to the data here
        self.push(data)
```

Earlier we mentioned node context. This comes into play when `run` is called
on the node, as the required and optional parts of the context are inferred from
the positional and keyword args of `run`. Take for example:

```python
class MyNode(Node):
    def run(self, data, conn, chunksize=None, **kwargs):
        # Some node-specific code here
        self.push(data)
```

All nodes expect their first positional arg to be the data going through the
pipeline. This node also requires a `conn` argument, and has an optional
`chunksize` argument. These values can be filled in from the following inputs
in priority order, with earlier methods overriding those further down the
list:

1\. Context args passed to `consume` for the particular node:
```python
conn = get_my_db_conn()
glider.consume(
    data,
    my_node=dict(conn=conn, chunksize=100)
)
```
2\. Default context set on the node at init time:
```python
conn = get_my_db_conn()
glider = Glider(
    MyNode("my_node", conn=conn, chunksize=100)
)
```
3\. Global pipeline state passed via `global_state`. This only works for populating positional args currently:
```python
conn = get_my_db_conn()
glider = Glider(
    MyNode("my_node"),
    global_state=dict(conn=conn)
)
```

<a name="cligeneration"></a>
CLI Generation
--------------

Glide allows creating parameterized command line scripts from any pipeline
with a simple decorator using the `Glider.cli` method. Consider the following simple
SQL extract and load pipeline:

```python
glider = Glider(
    SQLExtract("extract")
    | SQLLoad("load")
)
```

You can create a command line script from the `glider` object as follows:

```python
@glider.cli()
def main(data, node_contexts):
    glider.consume(data, **node_contexts)

if __name__ == "__main__":
    main()        
```

The script arguments, their types, and whether they are required or not is all
inferred by inspecting the `run` arguments on the nodes of the pipeline and
prefixing the node name. For example, `SQLLoad` requires a `conn` and a
`table` argument, as well as having a few optional arguments. Since the node
is named "load", the CLI will automatically generate required args called
`--load_conn` and `--load_table`. 

By default, the first positional argument(s) expected on the CLI is used to
populate the `data` argument. Additionally, the default help strings are
extracted from the run() method documentation if you use numpy docstring
format.

Let's ignore the fact that you can't pass a real
database connection object on the command line for a second and see how you
would run this script:

```shell
$ python my_script.py "select * from input_table limit 10" \
--extract_conn foo \
--load_conn bar \
--load_table output_table 
```

To pass multiple inputs to `data` you would simply use space-separated
positional arguments:

```shell
$ python my_script.py "sql query 1" "sql query 2" \
--extract_conn foo \
--load_conn bar \
--load_table output_table 
```

One way to populate the `conn` arguments of pipeline nodes is to define it in
the `global_state` or in the node initialization calls. In either case it is
no longer considered a *required* command line argument. So the following
would work:

```python
glider = Glider(
    SQLExtract("extract")
    | SQLLoad("load"),
    global_state=dict(conn=get_my_db_conn())
)
```

```shell
$ python my_script.py "select * from input_table limit 10" \
--load_table output_table 
```

### Blacklisting Args

In the previous example it is no longer necessary to even have the
node-specific connection arguments show up on the command line (such as in
--help output). You can blacklist the arg from ever getting put into the CLI
as follows:

```python
@glider.cli(blacklist=["conn"])
def main(data, node_contexts):
    glider.consume(data, **node_contexts)
```

Or, if you just wanted to blacklist an argument that appears in multiple nodes
from a single node (such as the `conn` argument required in both the extract
and load nodes in this example), you could be more explicit and prefix the
node name:

```python
@glider.cli(blacklist=["load_conn"])
def main(data, node_contexts):
    glider.consume(data, **node_contexts)
```

That would remove `load_conn` from the CLI but not `extract_conn`.

### Custom Arguments

You can also override or add any argument you want using the `Arg` class which
takes the standard `argparse` arguments:

```python
from glide.core import Glider, Arg

glider = ...

@glider.cli(Arg("--load_table", required=False, default="output_table"))
def main(data, node_contexts):
    glider.consume(data, **node_contexts)
```

And now, assuming you had used the `Glider` with `conn` passed in the
`global_state`, you could simple do:

```shell
$ python my_script.py "select * from input_table limit 10"
```

You can override the `data` positional argument in this way too if you want to
change the type/requirements:

```python
@glider.cli(Arg("data", type=str, default="some default sql query"))
def main(data, node_contexts):
    glider.consume(data, **node_contexts)
```

### Parent CLIs

If you want to inherit or share arguments you can accomplish that using the
`Parent` and `Arg` decorators together. These are using
[climax](https://github.com/miguelgrinberg/climax) under the hood, which is
utilizing `argparse`. For example, the following script inherits a `--dry_run`
boolean CLI flag:

```python
from glide.core import Parent, Arg

@Parent()
@Arg("--dry_run", action="store_true")
def parent_cli():
    pass

@glider.cli(parents=[parent_cli])
def main(data, dry_run=False, node_contexts):
    if dry_run:
        something_else()
    else:
        glider.consume(data, **node_contexts)
```

### Argument Injection and Clean Up

The script decorator also has the ability to inject values into arguments
based on the result of a function, and call clean up functions for the various
injected arguments. The following example shows two useful cases:

```python
def get_data():
    # do something to populate data iterable
    return data

@glider.cli(
    Arg("--load_table", required=False, default="output_table")
    inject=dict(data=get_data, conn=get_my_db_conn),
    cleanup=dict(conn=lambda x: x.close()),
)
def main(data, node_contexts, **kwargs):
    glider.consume(data, **node_contexts)
```

Here we use the `inject` decorator argument and pass a dictionary that maps
injected argument names to functions that return the values. We inject a `data`
arg and a `conn` arg and neither are necessary for the command line. This
automatically blacklists those args from the command line as well. Since we
added the `load_table` arg and gave it a default as well, we can now simply
run:

```shell
$ python my_script.py
```

> **Note:** Injected args are also passed to the wrapped function as keyword args. 

> **Also Note:** If an injected argument name is mapped to a non-function via
`inject` the value will be used as is. The main difference is those values are
interpreted as soon as the module is loaded (when the decorator is init'd). If
that is not desirable, pass a function as shown above which will only be
executed once the decorated function is actually called. Injected
RuntimeContexts and other objects that are not a `types.FunctionType` or
`functools.partial` are passed through as-is.

The `cleanup` decorator argument takes a dictionary that maps argument names to
callables that accept the argument value to perform some clean up. In this
case, it closes the database connection after the wrapped method is complete.

<a name="extensions"></a>
Extensions
----------

### Installing Extensions

To install all extensions and dev dependencies:

```shell
$ pip install glide[complete]
```

You can also just install Glide plus a specific extension:

```shell
$ pip install glide[dask]
$ pip install glide[celery]
$ pip install glide[rq]
$ pip install glide[swifter]
```

To access installed extensions import from the `glide.extensions` submodules
as necessary. Review the documentation and tests for current extensions for
help getting started. Note that the Pandas extension is currently included in
all Glide installs.

### Contributing Extensions

New extensions are encouraged! To add an extension:

1. Review the examples of other extensions in `glide.extensions`
2. Add tests for your extensions and don't forget to add support in `setup.py`
3. Review and follow the steps in [How to Contribute](#howtocontribute)

For inspiration, here are some ideas for extensions:

- NoSQL databases
- HTML Tables
- Google Ads
- Google Analytics
- Facebook Ads
- Bing Ads
- Salesforce

The idea is make it dead simple to extract data from X and load it to Y, and
reduce the amount of reinventing of the wheel that happens nowadays when it
comes to writing integrations with various data sources and destinations.

Note that some of the existing nodes/extensions may have the ability to
read/write data from more locations than you think. For example, some Pandas
extension nodes use read_csv/read_sql under the hood which can read from a
variety of source types. The SQL nodes can use SQLAlchemy, which also has its
own extensions to read from BigQuery, Redshift, or Spark SQL. Please consider
using the existing functionality and only add an extension if it is necessary
to overcome limitations.

<a name="documentation"></a>
Documentation
-------------

[Documentation](https://glide-etl.readthedocs.io/en/latest/) is a work in
progress. Most of the built-in nodes and pipelines are fairly self-explanatory
so you can supplement your knowledge by perusing the tests directory.

<a name="howtocontribute"></a>
How to Contribute
-----------------

1.  Check for open issues or open a new issue to start a discussion
    around a feature idea or a bug. 
2.  Fork [the repository](https://github.com/kmatarese/glide) on
    GitHub to start making your changes to the **master** branch (or
    branch off of it).
3.  Write a test which shows that the bug was fixed or that the feature
    works as expected.
4.  Send a [pull request](https://help.github.com/en/articles/creating-a-pull-request-from-a-fork). Add yourself to
    [AUTHORS](https://github.com/kmatarese/glide/blob/master/AUTHORS.rst).

In order to run tests you will need to set a GLIDE_CONFIG_FILE environment variable
that points to a .ini file containing information shown in tests/sample_config.ini.
