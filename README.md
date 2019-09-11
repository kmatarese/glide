Glide: Easy ETL
===============

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
- A simple decorator to generate a command line interface from a pipeline in ~one line of code
- The ability to control node contexts via defaults and/or simple runtime overrides

Installation
------------

> ⚠️ **Warning**: This project is still in an alpha state and should probably not be used in production.

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

`Glider` is the main pipeline class that takes a DAG of `Nodes` as input and
then accepts data to process in its `consume` method. In most cases `consume`
will iterate over its input as-is passing each item to the DAG to be
processed.

> **Note:** Some inputs, such as Pandas objects, strings, file objects, dicts,
and callables are automatically wrapped in a list to prevent them from being
broken up, as iteration is often inefficient or nonsensical in those cases.

The examples below assume you have used the following (taboo) shortcut to
import all necessary node and pipeline classes:

```python
from glide import *
```

The names of the built-in classes aim to be explicit and therefore can end up
a bit longer given the many combinations of ways to process data with Glide. As
a convention, nodes prefixed with "Row" expect to operate on plain old python
iterables, while nodes prefixed with "DataFrame" propagate Pandas DataFrames.

Let's build some pipelines to explore Glide further...

### Example: Read a CSV

Here is a trivial example that reads a CSV and passes all rows to a `Logger`
node in a single push to be pretty-printed:

```python
glider = Glider(
    RowCSVExtractor("extract")
    | Logger("load")
)
glider.consume(["/path/to/file.csv"])
```

### Example: DataFrame Transformation

Here is a slightly more realistic example applying a transformation to a
DataFrame read from a CSV, in this case lowercasing all strings before loading
into an output CSV:

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

### Node Context

The above example also demonstrates two separate ways to pass context to nodes:
        
1. Passing kwargs when instantiating the node. This becomes a default context
for the node any time it is used/reused.
2. Passing kwargs to `consume` that are node_name->node_context pairs. This context
lasts only for the the `consume` call. 

> **Note:** Further details can be found in the node creation documentation.

> **Also Note:** Many of the provided nodes pass their context to
well-documented functions, such as `DataFrame.to_csv` in the case of
`DataFrameCSVLoader`. Review the documentation/code for each node for more
detail on how args are processed and which are required.

### Example: DataFrame Transformation (Parallel)

Let's do the same thing with the data split in parallel processes using a
`ProcessPoolExecutor` at the transformation step. Note that we instead use a
`DataFrameProcessPoolTransformer` and adjusted the `func` argument to the
transformer since it operates on a chunk of the DataFrame instead of being fed
individual elements from the DataFrame as `apply_map` does under the hood in
the previous example:

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

### Example: Placeholder Nodes

You can also easily drop replacement nodes into a templated pipeline. In this
case we use a `PlaceholderNode` for the extract node in the pipeline
definition and then replace that with a `DataFrameCSVExtractor`. The result is
a pipeline that can extract a CSV from one file, perform some custom
transformation on the DataFrame, and then load it to another CSV.

```python
glider = Glider(
    PlaceholderNode("extract"),
    | MyTransformer("transform")
    | DataFrameCSVLoader("load", index=False, mode="a")
)
glider["extract"] = DataFrameCSVExtractor("extract")
glider.consume(
    ["/path/to/infile.csv"],
    extract=dict(chunksize=100),
    load=dict(outfile="/path/to/outfile.csv")
)
```

> **Note:** Any node can be replaced by name. `PlaceholderNode` is just a convenience.

### Example: Global State

A `Glider` can also have a shared context that can be used to populate node
arguments via its optional `global_state` argument:

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
`ParaGlider` (who said ETL isn't fun?!?) instead of a `Glider`. The following
code will create a process pool and split processing of the inputs over the
pool, with each process running the entire pipeline on part of the consumed
data:

```python
glider = ProcessPoolParaGlider(
    RowCSVExtractor('extract') |
    Logger('load')
)
glider.consume(
    ["/path/to/infile1.csv", "/path/to/infile2.csv"],
    extract=dict(nrows=50)
)
```

### Example: Parallel Branching

If you don't want to execute the entire pipeline in parallel, you can also
branch into parallel execution in the middle of the DAG utilizing a parallel
push node as in the following example:

```python
glider = Glider(
    RowCSVExtractor("extract", nrows=60)
    | ProcessPoolPush("push", split=True)
    | [Logger("load1"), Logger("load2"), Logger("load3")]
)
glider.consume(["/path/to/infile.csv"])
```

The above example will extract 60 rows from a CSV and then push equal slices
to the logging nodes in parallel processes. Using `split=False` (default)
would have passed the entire 60 rows to each logging node in parallel
processes. 

> **Note:** Once you branch off into processes there is currently no way to
reduce/join the pipeline back into the original process and resume
single-process operation on the multiprocessed results. However, that can be
achieved with threads if necessary as shown in the next example.

### Example: Thread Reducers

```python
glider = Glider(
    RowCSVExtractor("extract", nrows=60)
    | ThreadPoolPush("push", split=True)
    | [Logger("load1"), Logger("load2"), Logger("load3")]
    | ThreadReducer("reducer")
    | Logger("loadall")
)
glider.consume(["/path/to/infile.csv"])
```

The above code will split the data and push to the first 3 logging nodes in
multiple threads. The `ThreadReducer` won't push until all of the previous
nodes have finished, and then the final logging node will print all of the
results.

### Summary of Parallel Processing

At this point it's worth summarizing the various ways you can attempt parallel processing
using Glide:

- Method 1: Parallelization *within* nodes such as `DataFrameProcessPoolTransformer`
- Method 2: Completely parallel pipelines via `ParaGliders` (each process executes the entire pipeline)
- Method 3: Branched parallelism using parallel push nodes such as `ProcessPoolPush` or `ThreadPoolPush`

Each has its own utility and/or quirks. Method 1 is perhaps the most
straightforward since you return to single process operation after the node is
done doing whatever it needed to do in parallel, though the shuffling of data
to/from subprocesses is not without cost. Method 2 may be useful and easy to
understand in certain cases as well. Method 3 can lead to more
complex/confusing flows and should likely only be used towards the end of
pipelines to branch the output in parallel, such as if writing to several
databases in parallel as a final step.

> **Note:** combining the approaches may not work and has not been tested.

> **Also Note:** standard limitations apply regarding what types of data can
be serialized and passed to a parallel process.

Creating Nodes
--------------

Creating nodes is quite simple. You must inherit from the Glide `Node` class
and you must define a `run` method that takes at least one positional argument
for the data being pushed to it. The `run` method should call
`self.push(data)` with the data it wants to push downstream.

Here is an example of a simple transformer node:

```python
from glide import Node

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
3\. Global pipeline state passed via `global_state`. This only works for positional args currently:
```python
conn = get_my_db_conn()
glider = Glider(
    MyNode("my_node"),
    global_state=dict(conn=conn)
)
```

CLI Generation
--------------

Glide allows creating parameterized command line scripts from any pipeline
with a simple decorator using the `Glider.cli` method. Consider the following simple
SQL extract and load pipeline:

```python
glider = Glider(
    RowSQLExtractor("extract")
    | RowSQLLoader("load")
)
```

You can create a command line script from the `glider` object as follows:

```python
@glider.cli()
def main(data, **node_contexts):
    glider.consume(data, **node_contexts)

if __name__ == "__main__":
    main()        
```

The script arguments, their types, and whether they are required or not is all
inferred by inspecting the `run` arguments on the nodes of the pipeline and
prefixing the node name. For example, `RowSQLLoader` requires a `conn` and a
`table` argument, as well as having a few optional arguments. Since the node
is named "load", the CLI will automatically generate required args called
`--load_conn` and `--load_table`. 

By default, the first positional argument(s) expected on the CLI is used to
populate the `data` argument. Let's ignore the fact that you can't pass a real
database connection object on the command line for a second and see how you
would run this script:

```bash
python my_script.py "select * from input_table limit 10" --extract_conn foo --load_conn bar --load_table output_table 
```

To pass multiple inputs to `data` you would simply do use space-separated
positional arguments:

```bash
python my_script.py "sql query 1" "sql query 2" --extract_conn foo --load_conn bar --load_table output_table 
```

One way to populate the `conn` arguments of pipeline nodes is to define it in
the `global_state` or in the node initialization calls. In either case it is
no longer considered a *required* command line argument. So the following
would work:

```python
glider = Glider(
    RowSQLExtractor("extract")
    | RowSQLLoader("load"),
    global_state=dict(conn=get_my_db_conn())
)
```

```bash
python my_script.py "select * from input_table limit 10" --load_table output_table 
```

### Blacklisting Args

In the previous example it is no longer necessary to even have the node-specific connection
arguments show up on the command line. You can blacklist the arg from ever getting put
into the CLI as follows:

```python
@glider.cli(blacklist=['conn'])
def main(data, **node_contexts):
    glider.consume(data, **node_contexts)
```

Or, if you just wanted to blacklist an argument that appears in multiple nodes
from a single node (such as the `conn` argument required in both the extract
and load nodes in this example), you could be more explicit and prefix the
node name:

```python
@glider.cli(blacklist=['load_conn'])
def main(data, **node_contexts):
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
def main(data, **node_contexts):
    glider.consume(data, **node_contexts)
```

And now, assuming you had used the `Glider` with `conn` passed in the
`global_state`, you could simple do:

```bash
python my_script.py "select * from input_table limit 10"
```

You can override the `data` positional argument in this way too if you want to
change the type/requirements:

```python
@glider.cli(Arg("data", type=str, default="some default sql query"))
def main(data, **node_contexts):
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
def main(data, dry_run=False, **node_contexts):
    if dry_run:
        something_else()
    else:
        glider.consume(data, **node_contexts)
```

### Argument Injection and Clean-up

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
    clean=dict(conn=lambda x: x.close()),
)
def main(data, conn, **node_contexts):
    glider.consume(data, **node_contexts)
```

Here we use the `inject` decorator argument and pass a dictionary that maps
injected argument names to callables that return the value. We inject a `data`
arg and a `conn` arg and neither are necessary for the command line. This
automatically blacklists those args from the command line as well. Since we added
the `load_table` arg and gave it a default as well, we can now simply run:

```bash
python my_script.py
```

Note that injected args are also passed to the wrapped function. The `clean`
decorator argument takes a dictionary that maps argument names to callables
that accept the argument value to perform some clean up. In this case, it
closes the database connection after the wrapped method is complete.

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
