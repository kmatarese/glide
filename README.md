Glide: A data processing / ETL pipeline tool
=========================================

[![Generic badge](https://img.shields.io/badge/Status-Alpha-yellow.svg)](https://shields.io/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Documentation Status](https://readthedocs.org/projects/glide-etl/badge/?version=latest)](https://glide-etl.readthedocs.io/en/latest/?badge=latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Introduction
------------

Glide was inspired by and uses a similar syntax to [Consecution](https://github.com/robdmc/consecution), which is an easy-to-use
pipeline abstraction tool inspired by [Apache Storm Topologies](http://storm.apache.org/releases/current/Tutorial.html).
Like those libraries, Glide is:

- A simple, reusable approach to building robust ETL pipelines
- A system for wiring together processing nodes to form a DAG

Glide also has: 

- An expanding suite of built-in nodes and pipelines that cover common use cases, such as extracting and loading data from/to SQL/CSVs/Excel/URLs
- Built-in nodes for Pandas DataFrame-based pipelines, including optional support for DataFrame transformation via [Dask](https://dask.org/) or [Swifter](https://github.com/jmcarpenter2/swifter)
- A variety of node and DAG parallel processing strategies via concurrent.futures Executors or optional [Dask](https://dask.org/) support
- The ability to control node contexts via defaults and/or simple runtime overrides

> ⚠️ **Warning**: This project is still in an alpha state and should probably not be used in production.

Installation
------------

Until this gets added to PyPi, you must clone the git repo and then install into your python
environment as follows:

```shell
git clone https://github.com/kmatarese/glide
cd glide
source /path/to/venv/bin/activate
pip install -r requirements.txt
make ENV=/path/to/venv install # Or "make ENV=/path/to/venv develop" for development
```

Examples
--------

The following examples serve as a quickstart to illustrate some core features
and built-in nodes. More complete documentation is in progress and can be
viewed [here](https://glide-etl.readthedocs.io/en/latest/index.html).

```python
from glide import *
```

Glider is the main pipeline class that takes a DAG of nodes as input, and then
accepts data to process in its consume() method. 

> **Note:** In most cases consume() will iterate over its input as-is passing
each item to the DAG to be processed. Some inputs, such as Pandas objects,
strings, file objects, dicts, and callables are automatically wrapped in a
list to prevent them from being broken up since iteration is often inefficient
or nonsensical in those cases.

### Example: Read a CSV

Here is a simple example that reads a CSV and logs rows to stdout:

```python
glider = Glider(
    RowCSVExtractor("extract")
    | LoggingLoader("load")
)
glider.consume(["/path/to/file.csv"])
```

### Example: Placeholder Nodes

You can also easily drop replacement nodes into a templated pipeline. In this
case we use a PlaceholderNode for the extract node and then replace that with
a DataFrameCSVExtractor. 

```python
glider = Glider(
    PlaceholderNode("extract")
    | DataFrameCSVLoader("load", index=False, mode="a")
)
glider["extract"] = DataFrameCSVExtractor("extract")
glider.consume(
    ["/path/to/infile.csv"],
    extract=dict(chunksize=100),
    load=dict(outfile="/path/to/outfile.csv")
)
```

### Node Context

The above example also demonstrates two separate ways to pass context to nodes:
        
1. Passing kwargs when instantiating the node. This becomes a default context
for the node any time it is used/reused.
2. Passing kwargs to consume that are node_name->node_context pairs. This context
lasts only for the the consume() call. 

> **Note:** Further details can be found in the node creation
documentation. Many of the provided nodes pass their context to
well-documented functions, such as DataFrame.to_csv in the case of
DataFrameCSVLoader. Review the documentation/code for each node for more
detail on how args are processed and which are required.

### Example: DataFrame Transformation

Here is an example applying a transformation to a DataFrame read from a CSV,
in this case lowercasing all strings before loading into an output CSV:

```python
def lower(s):
    return s.lower() if type(s) == str else s

glider = Glider(
    DataFrameCSVExtractor("extract")
    | DataFrameApplyMapTransformer("transform")
    | DataFrameCSVLoader("load", index=False, mode="a")
)
glider.consume(
    ["/path/to/infile.csv"],
    extract=dict(chunksize=100),
    transform=dict(func=lower),
    load=dict(outfile="/path/to/outfile.csv"),
)
```

### Example: DataFrame Transformation (Parallel)

Let's do the same thing with the data split in parallel processes using a
ProcessPoolExecutor at the transformation step. Note that we instead use a
DataFrameProcessPoolTransformer and adjusted the func argument to the
transformer since it operates on a chunk of the DataFrame.

```python
def lower(s):
    return s.lower() if type(s) == str else s

def df_lower(df):
    df = df.applymap(lower)
    return df

glider = Glider(
    DataFrameCSVExtractor("extract")
    | DataFrameProcessPoolTransformer("transform")
    | DataFrameCSVLoader("load", index=False, mode="a")
)
glider.consume(
    ["infile.csv"],
    transform=dict(func=df_lower),
    load=dict(outfile="outfile.csv"),
)
```

> **Note:** there are transformer nodes for using Swifter and Dask as well if
you install those extensions.

### Example: Global State

A Glider can also have a shared context that can be used to populate node
arguments via its optional global_state arg:

```python
conn = get_my_sqlalchemy_conn()
sql = "select * from in_table limit 10"

glider = Glider(
    DataFrameSQLExtractor("extract")
    | DataFrameSQLLoader("load", if_exists="replace", index=False),
    global_state=dict(conn=conn) # conn will automagically be passed to any nodes that require it
)
glider.consume(
    [sql],
    load=dict(table="out_table")
)
```

### Example: Parallel Pipelines via ParaGlider

Glide also has support for completely parallelizing pipelines using a
ParaGlider (who said ETL isn't fun?!?) instead of a Glider. The following code
will create a process pool and split processing of the inputs over the pool,
with each process running the entire pipeline on part of the consumed data:

```python
glider = ProcessPoolParaGlider(
    RowCSVExtractor('extract') |
    LoggingLoader('load')
)
glider.consume(
    ["/path/to/infile1.csv", "/path/to/infile2.csv"],
    extract=dict(nrows=50)
)
```

### Example: Parallel Branching

If you don't want to execute the entire glider pipeline in parallel, you can
also branch into parallel execution utilizing a parallel push node as in the
following example:

```python
glider = Glider(
    RowCSVExtractor("extract", nrows=60)
    | ProcessPoolPush("push", split=True)
    | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
)
glider.consume(["/path/to/infile.csv"])
```

The above example will extract 60 rows from a CSV and then push equal slices
to the logging nodes in parallel processes. Using split=False (default) would
have passed the entire 60 rows to each logging node in parallel
processes. Note that once you branch off into processes there is currently no
way to reduce/join the pipeline back into the original process and resume
single-process operation on the multiprocessed results. However, that can be
achieved with threads if necessary as shown in the next example.

### Example: Thread Reducers

```python
glider = Glider(
    RowCSVExtractor("extract", nrows=60)
    | ThreadPoolPush("push", split=True)
    | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
    | ThreadReducer("reducer")
    | LoggingLoader("loadall")
)
glider.consume(["/path/to/infile.csv"])
```

The above code will split the data and push to the first 3 logging nodes in
multiple threads. The ThreadReducer won't push until all of the previous nodes
have finished, and then the final logging node will print all of the results.

### Summary of Parallel Processing

At this point it's worth summarizing the various ways you can attempt parallel processing
using Glide:

- Method 1: Parallelization *within* nodes such as DataFrameProcessPoolTransformer 
- Method 2: Completely parallel pipelines via ParaGliders (each process executes the entire pipeline)
- Method 3: Branched parallelism using parallel push nodes such as ProcessPoolPush or ThreadPoolPush

Each has its own utility and/or quirks. Method 1 is perhaps the most
straightforward since you return to single process operation after the node is
done doing whatever it needed to do in parallel, though the shuffling of data
to/from subprocesses is not without cost. Method 2 may be useful and easy to
understand in certain cases as well. Method 3 can lead to more
complex/confusing flows and should likely only be used towards the end of
pipelines to branch the output in parallel, such as if writing to several
databases in parallel.

> **Note:** combining the approaches may not work and has not been tested.

> **Also Note:** standard limitations apply regarding what types of data can be serialized and passed to a parallel process.

Creating Nodes
--------------

Creating nodes is quite simple. You must inherit from the Glide Node class and
you must define a run() method that takes at least one positional argument for
the data being pushed to it. The run() method should call self.push() with the
data it wants to push downstream.

Here is an example of a simple transformer node:

```python
from glide import Node

class ExampleTransformer(Node):
    def run(self, item):
        # Do something to item here
        self.push(item)
```

Earlier we mentioned node context. This comes into play when run() is called
on the node, as the required and optional parts of the context are inferred from
the positional and keyword args of run(). Take for example:

```python
class MyNode(Node):
    def run(self, item, conn, chunksize=None, **kwargs):
        # Some node-specific code here
        self.push(item)
```

All nodes expect their first positional arg to be the data going through the
pipeline. This node also requires a 'conn' argument, and has an optional
'chunksize' argument. These values can be filled in from the following inputs
in priority order, with earlier methods overriding those further down the
list:

1. Context args passed to consume for the node:
    ```python
    conn = get_my_db_conn()
    glider.consume(
        data,
        my_node=dict(conn=conn, chunksize=100)
    )
    ```
2. Default context set on the node at init time:
    ```python
    conn = get_my_db_conn()
    glider = Glider(
        MyNode("my_node", conn=conn, chunksize=100)
    )
    ```
3. Global pipeline state passed via global_state. This only works for positional args currently:
    ```python
    conn = get_my_db_conn()
    glider = Glider(
        MyNode("my_node"),
        global_state=dict(conn=conn)
    )
    ```

Documentation
-------------

[Documentation](https://glide-etl.readthedocs.io/en/latest/) is a work in
progress. Most of the built-in nodes and pipelines are fairly self-explanatory
so you can supplement your knowledge by perusing the tests directory.

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
