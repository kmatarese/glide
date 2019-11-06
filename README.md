Glide: Easy ETL
===============

[![Generic badge](https://img.shields.io/badge/Status-Alpha-yellow.svg)](https://shields.io/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Documentation Status](https://readthedocs.org/projects/glide-etl/badge/?version=latest)](https://glide-etl.readthedocs.io/en/latest/?badge=latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Introduction
------------

Glide is an easy-to-use data pipelining tool inspired by [Consecution](https://github.com/robdmc/consecution) and 
[Apache Storm Topologies](http://storm.apache.org/releases/current/Tutorial.html). 

Like those libraries, **Glide is:**

- A simple, reusable approach to building robust ETL pipelines
- A system for wiring together processing nodes to form a directed acyclic graph (DAG)

**Glide also has:**

- An expanding suite of built-in nodes and pipelines that extract, transform, and load data from/to any combination of:
  - SQL databases (SQLite, DBAPI, and SQLAlchemy support)
  - Local or remote files (CSVs, Excel, and raw file support)
  - URLs (JSON endpoints, file downloads, APIs, etc.)
  - HTML Tables
  - Emails
- Extensions for [Pandas](https://pandas.pydata.org/), [Dask](https://dask.org/), [Celery](http://www.celeryproject.org/), [Redis Queue](http://python-rq.org/) and more
- A variety of node and DAG parallel/distributed processing strategies
- A simple decorator to generate a command line interface from a pipeline in ~one line of code
- Flexible pipeline templating

**Glide is not** a task orchestration and/or dependency management tool like
Airflow. Use Glide to define your easily developed/contained/reusable/testable
data processing pipelines and then rely on a tool like Airflow to do what it's
good at, namely scheduling and complex task dependency management.

Table of Contents
-----------------

* [Installation](#installation)
* [Primer](#primer) 
* [Examples](#examples)
  * [CSV Extract, Transform, and Load](#example-csv-extract-transform-and-load)
  * [SQL Extract and Load](#example-sql-extract-and-load)
  * [SQL Transactions](#example-sql-transactions)
  * [DataFrames](#example-dataframes)
  * [URL Extraction](#example-url-extraction)
  * [JSON Converters](#example-json-converters)
  * [Filters](#example-filters)
  * [IterPush](#example-iterpush)
  * [SplitPush](#example-splitpush)
  * [SplitByNode](#example-splitbynode)
  * [Window Processing](#example-window-processing)
  * [Date Windows](#example-date-windows)
  * [Parallel Transformation](#example-parallel-transformation)
  * [Parallel Pipelines via ParaGlider](#example-parallel-pipelines-via-paraglider)
  * [Parallel Branching](#example-parallel-branching)
  * [Thread Reducers](#example-thread-reducers)
  * [Templated Nodes and Pipelines](#example-templated-nodes-and-pipelines)
  * [Data Integrity Checks](#example-data-integrity-checks)
  * [Debugging](#example-debugging)
  * [Profiling Pipelines](#example-profiling-pipelines)
  * [Complex Pipelines](#example-complex-pipelines)
  * [Plotting Pipeline DAGs](#example-plotting-pipeline-dags)
* [CLI Generation](#cli-generation)
* [Extensions](#extensions)
  * [Pandas](#pandas)
  * [Dask](#dask-experimental)
  * [Celery](#celery-experimental)
  * [Redis Queue](#redis-queue-experimental)
  * [Swifter](#swifter-experimental)
* [Docs](#documentation)
* [How to Contribute](#how-to-contribute)

<a name="installation"></a>
Installation
------------

> ⚠️ **Warning**: This project is in an alpha state and is rapidly changing. It should not be used in production.

```shell
$ pip install glide
```

<a name="primer"></a>
Primer
------

You are encouraged to take a deeper look at the [docs](#documentation), but
the short of it is the following:

1\. A `Node` is a part of a pipeline which has a `run` method that typically
accepts data from upstream nodes, and pushes data to downstream nodes. For example:
```python
class MyNode(Node):
    def run(self, data):
        # Some node-specific code here
        self.push(data)
```
2\. A `Glider` is a pipeline of `Node` objects wired together in a DAG. It
accepts input data in its `consume` method. For example:
```python
glider = Glider(
    MyExtractNode("extract")
    | MyTransformNode("transform")
    | MyLoadNode("load")
)
glider.consume(data)
```

If a node's `run` method has additional parameters, they are populated from
the node's `context`. More info on creating nodes and populating runtime context 
can be found [here](https://glide-etl.readthedocs.io/en/latest/nodes.html).

<a name="examples"></a>
Examples
--------

The following examples serve to quickly illustrate some core features and
built-in nodes. There is much more `Glide` can do that is not shown
here. Everything below assumes you have used the following shortcut to import
all necessary node and pipeline classes:

```python
from glide import *
```

<a name="example-csv-extract-transform-and-load"></a>
### Example: CSV Extract, Transform, and Load

Apply a transformation to data from a CSV, use a function to lowercase all
strings, and load into an output CSV:

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

<a name="example-sql-extract-and-load"></a>
### Example: SQL Extract and Load

Read from one table, write to another:

```python
conn = get_my_sqlalchemy_conn()
sql = "select * from in_table limit 10"

glider = Glider(
    SQLExtract("extract")
    | SQLLoad("load"),
    global_state=dict(conn=conn) # conn is automagically passed to any nodes that accept a "conn" argument
)
glider.consume(
    [sql],
    load=dict(table="out_table")
)
```

<a name="example-sql-transactions"></a>
### Example: SQL Transactions

Start a transaction before writing to a database, rollback on failure:

```python
glider = Glider(
    SQLExtract("extract")
    | SQLTransaction("tx")
    | SQLLoad("load", rollback=True),
    global_state=dict(conn=conn)
)
glider.consume(...)
```

<a name="example-dataframes"></a>
### Example: DataFrames

The Pandas extension allows you to operate with DataFrames:

```python
def lower(s):
    return s.lower() if type(s) == str else s

glider = Glider(
    DataFrameCSVExtract("extract")
    | DataFrameApplyMap("transform", func=lower)
    | DataFrameCSVLoad("load", index=False, mode="a")
)
glider.consume(...)
```

See the extension tests and code for more examples and documentation.

<a name="example-url-extraction"></a>
### Example: URL Extraction

Extract data from each URL in the list of requests and load to a URL endpoint:

```python
glider = Glider(URLExtract("extract") | URLLoad("load"))
reqs = [
    "https://jsonplaceholder.typicode.com/todos/1",
    "https://jsonplaceholder.typicode.com/todos/2",
]
glider.consume(
    reqs,
    extract=dict(data_type="json"),
    load=dict(
        url="https://jsonplaceholder.typicode.com/todos",
        data_param="json",
        headers={"Content-type": "application/json; charset=UTF-8"},
    ),
)
```

<a name="example-json-converters"></a>
### Example: JSON Converters

Load JSON from a string:

```python
glider = Glider(URLExtract("extract") | JSONLoads("json"))
reqs = ["https://jsonplaceholder.typicode.com/todos/1"]
glider.consume(reqs, extract=dict(data_type="text"))
```

Or dump it to a string with the `JSONDumps` node.

<a name="example-filters"></a>
### Example: Filters

Filter the propagation of data based on the result of a function:

```python
def data_check(node, data):
    # do some check on data, return True/False to control filtering
    return True

glider = Glider(
    MyExtract("extract")
    | Filter("filter", func=data_check)
    | MyLoad("load")
)
```

<a name="example-iterpush"></a>
### Example: IterPush

Push each row of an input iterable individually:

```python
glider = Glider(
    CSVExtract("extract", nrows=20)
    | IterPush("iter")
    | Print("load")
)
```

<a name="example-splitpush"></a>
### Example: SplitPush

Split an iterable before pushing:

```python
glider = Glider(SplitPush("push", split_count=2) | Print("print"))
glider.consume([range(4)])
```

<a name="example-splitbynode"></a>
### Example: SplitByNode

Split an iterable evenly among downstream nodes:

```python
glider = Glider(SplitByNode("push") | [Print("print1"), Print("print2")])
glider.consume([range(4)])
```

<a name="example-window-processing"></a>
### Example: Window Processing

Push a sliding window of the data:

```python
glider = Glider(
    CSVExtract("extract", nrows=5)
    | WindowPush("window", size=3)
    | MyWindowCalcNode("calc")
)
```

<a name="example-date-windows"></a>
### Example: Date Windows

Generate a set of date windows and push them downstream:

```python
import datetime

today = datetime.date.today()
glider = Glider(DateTimeWindowPush("windows") | PrettyPrint("print"))
glider.consume(
    None,
    windows=dict(
        start_date=today - datetime.timedelta(days=3), end_date=today, num_windows=2
    )
)
```

Or use `DateWindowPush` for date objects. Note that `None` is passed as the
first arg to `consume` because the top node is a special `NoInputNode` that
takes no input data and generates data to push on its own.

<a name="example-parallel-transformation"></a>
### Example: Parallel Transformation

Call a function in parallel processes on equal splits of data from a CSV:

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

<a name="example-parallel-pipelines-via-paraglider"></a>
### Example: Parallel Pipelines via ParaGlider

Completely parallelize a pipeline using a `ParaGlider` (who said ETL isn't
fun?!?). Split processing of the inputs (two files in this case) over the
pool, with each process running the entire pipeline on part of the consumed
data:

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

<a name="example-parallel-branching"></a>
### Example: Parallel Branching

Branch into parallel execution in the middle of the DAG utilizing a parallel
push node:

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
each subprocess. However, that is possible with threads as shown in the next
example.

<a name="example-thread-reducers"></a>
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

<a name="example-templated-nodes-and-pipelines"></a>
### Example: Templated Nodes and Pipelines

Drop replacement nodes into an existing pipeline. Any node can be replaced by name:

```python
glider = Glider(
    PlaceholderNode("extract")
    | CSVLoad("load")
)
glider["extract"] = CSVExtract("extract")
glider.consume(...)
```

Or reuse an existing structure of nodes with a `NodeTemplate`:

```python
nodes = NodeTemplate(
    CSVExtract("extract")
    | CSVLoad("load")
)
glider = Glider(nodes()) # Copy of nodes created with each call
```

Or reuse an existing pipeline structure with `GliderTemplate`:

```python
template = GliderTemplate(
    CSVExtract("extract")
    | CSVLoad("load")
)
glider = template() # Copy of pipeline created with each call
```

<a name="example-data-integrity-checks"></a>
### Example: Data Integrity Checks

You can use the `AssertFunc` node to assert that some condition of the data is
met:

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

Similarly, you can do a sql-based check with `AssertSQL`, in this case simply
verifying the number of rows inserted:

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
have it do a comparison to the result of some function of the data:

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

<a name="example-debugging"></a>
### Example: Debugging

To enable debug logging for Glide change the log level of the "glide" logger:

```python
import logging
logging.getLogger("glide").setLevel(logging.DEBUG)
```

Glide will then print debug information about data passed through your
pipeline. 

You can also pass `_log=True` to the init method of any node to enable logging
of processed data:

```python
glider = Glider(
    SQLExtract("extract", _log=True)
    ...
)
```

Additionaly, you can pass `_debug=True` to the init method of any node to
cause the node to drop into PDB right before calling `run`, assuming you
aren't executing the pipeline in a subprocess:

```python
glider = Glider(
    SQLExtract("extract", _debug=True)
    ...
)
```

Finally, there are a variety of print nodes you can place in your pipeline for
general logging or debugging, such as `Print`, `PrettyPrint`, `LenPrint`,
`ReprPrint`, and `FormatPrint`. See the node documentation for more info.

<a name="example-profiling-pipelines"></a>
### Example: Profiling Pipelines

Insert a `Profile` node somewhere in your pipeline to get profiler information
for all downstream nodes:

```python
glider = Glider(
    Profile("profile")
    ...
)
```

<a name="example-complex-pipelines"></a>
### Example: Complex Pipelines

If the hierarchy of nodes you want to form is not achievable with the `|`
operator, you can use the `add_downstream` `Node` method to form more complex
graphs. More info can be found
[here](https://glide-etl.readthedocs.io/en/latest/pipelines.html#creating-pipelines).

<a name="example-plotting-pipeline-dags"></a>
### Example: Plotting Pipeline DAGs

If you have the [Graphviz](http://www.graphviz.org/) package installed, you can generate
a plot of your pipelines by simply doing the following:

```python
glider = Glider(...)
glider.plot("/path/to/filename.png")
```

<a name="cli-generation"></a>
CLI Generation
--------------

With Glide you can create parameterized command line scripts from any pipeline
with a simple decorator:

```python
glider = Glider(
    SQLLoad("extract")
    | SQLExtract("load")
)

@glider.cli()
def main(data, node_contexts):
    glider.consume(data, **node_contexts)

if __name__ == "__main__":
    main()
```

The script arguments, their types, and whether they are required or not is all
inferred by inspecting the `run` arguments on the nodes of the pipeline and
prefixing the node name. Please see the full documentation
[here](https://glide-etl.readthedocs.io/en/latest/pipelines.html#cli-generation)
for more details.

<a name="extensions"></a>
Extensions
----------

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
help getting started.

<a name="pandas"></a>
### Pandas

Note that the Pandas extension is actually support by default with all `glide` installs.

See the extension docs
[here](https://glide-etl.readthedocs.io/en/latest/glide.extensions.pandas.html)
for node/pipeline reference information.

See the tests
[here](https://github.com/kmatarese/glide/tree/master/tests/pandas_ext/test_pandas.py)
for some additional examples.

<a name="dask-experimental"></a>
### Dask - Experimental

See the extension docs
[here](https://glide-etl.readthedocs.io/en/latest/glide.extensions.dask.html)
for node/pipeline reference information.

See the tests
[here](https://github.com/kmatarese/glide/tree/master/tests/dask_ext/test_dask.py)
for some additional examples.

<a name="celery-experimental"></a>
### Celery - Experimental

See the extension docs
[here](https://glide-etl.readthedocs.io/en/latest/glide.extensions.celery.html)
for node/pipeline reference information.

See the tests
[here](https://github.com/kmatarese/glide/tree/master/tests/celery_ext/test_celery.py)
for some additional examples.

<a name="redis-queue-experimental"></a>
### Redis Queue - Experimental

See the extension docs
[here](https://glide-etl.readthedocs.io/en/latest/glide.extensions.rq.html)
for node/pipeline reference information.

See the tests
[here](https://github.com/kmatarese/glide/tree/master/tests/rq_ext/test_rq.py)
for some additional examples.

<a name="swifter-experimental"></a>
### Swifter - Experimental

See the extension docs
[here](https://glide-etl.readthedocs.io/en/latest/glide.extensions.swifter.html)
for node/pipeline reference information.

See the tests
[here](https://github.com/kmatarese/glide/tree/master/tests/swifter_ext/test_swifter.py)
for some additional examples.

<a name="documentation"></a>
Documentation
-------------

More thorough documentation can be found [here](https://glide-etl.readthedocs.io/en/latest/).
You can supplement your knowledge by perusing the [tests](https://github.com/kmatarese/glide/tree/master/tests) directory 
or the [module reference](https://glide-etl.readthedocs.io/en/latest/glide.html).

<a name="how-to-contribute"></a>
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

### Contributing Extensions

To add an extension:

1. Review the examples of other extensions in `glide.extensions`
2. Add tests for your extensions and don't forget to add support in `setup.py`
3. Review and follow the steps in [How to Contribute](#how-to-contribute)

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
