Glide: A data processing / ETL pipeline tool
=========================================

[![Generic badge](https://img.shields.io/badge/Status-Alpha-yellow.svg)](https://shields.io/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Documentation Status](https://readthedocs.org/projects/glide-etl/badge/?version=latest)](https://glide-etl.readthedocs.io/en/latest/?badge=latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Glide uses a syntax similar to [Consecution](https://github.com/robdmc/consecution), which was inspired by Apache Storm Topologies.
Glide basically wraps the functionality of Consecution to provide some utilities out of the box:
        
- Reading/Writing to/from CSVs, Excel, or SQL Databases
- Nodes for processing row-based iterators (such as a list of dicts) or Pandas DataFrames
- Nodes utilizing [Swifter](https://github.com/jmcarpenter2/swifter) or [Dask](https://github.com/dask/dask) for DataFrame transformation
- Node and graph parallelization support via concurrent.futures Executors or Dask Client/Delayed/DataFrames
- Ability to set default node contexts which can be easily overwritten when calling glider.consume()

> ⚠️ **Warning**: This project is still in an alpha state. Your mileage may vary.

Installation
------------

Currently you must clone the git repo and then install into your python
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

```python
from glide import *
```

A simple example that reads a CSV and logs rows to stdout:

```python
glider = Glider(
    RowCSVExtractor("extract")
    | LoggingLoader("load")
)
glider.consume(["filename.csv"])
```

An example of using and replacing placeholders in glider pipelines, which may
be useful if you want to drop replacement nodes into a templated pipeline:

```python
glider = Glider(
    PlaceholderNode("extract")
    | DataFrameCSVLoader("load", index=False, mode="a")
)
glider["extract"] = DataFrameCSVExtractor("extract")
glider.consume(
    ["infile.csv"],
    extract=dict(chunksize=100),
    load=dict(outfile="outfile.csv")
)
```

The above example also demonstrates two separate ways to pass context to nodes:
        
1. Passing kwargs when instantiating the node. This becomes a default context
for the node any time it is used/reused.
2. Passing kwargs to consume that are node_name->node_context pairs. This context
lasts only for the the consume() call. 

Further details can be found in the node creation docs. Many of the provided
nodes pass their context to well-documented functions, such as
DataFrame.to_csv in the case of DataFrameCSVLoader. Review the
documentation/code for each node for more detail on how args are processed and
which are required.

Back to examples...

An example applying a transformation to a DataFrame.

```python
def lower(s):
    return s.lower() if type(s) == str else s

glider = Glider(
    DataFrameCSVExtractor("extract")
    | DataFrameApplyMapTransformer("transform")
    | DataFrameCSVLoader("load", index=False, mode="a")
)

glider.consume(
    ["infile.csv"],
    extract=dict(chunksize=100),
    transform=dict(func=lower),
    load=dict(outfile="outfile.csv"),
)
```

Or have the glider split the DataFrame and do it in parallel:

```python
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

A glider can also have shared/global context that can be used to populate node arguments:

```python
conn = get_my_sqlalchemy_conn()
sql = "select * from in_table limit 10"
glider = Glider(
    DataFrameSQLExtractor("extract")
    | DataFrameSQLLoader("load", if_exists="replace", index=False),
    global_state=dict(conn=conn) # conn will automagically be passed to any nodes that require it
)
glider.consume([sql], load=dict(table="out_table"))
```

Glide also has support for completely parallelizing glider pipelines using a
ParaGlider (HA!) instead of a Glider. The following code will create a process
pool and split processing of the inputs over the pool, with each process
running the entire glider pipeline on part of the consumed data:

```python
glider = ProcessPoolParaGlider(
    RowCSVExtractor('extract') |
    LoggingLoader('load')
)
glider.consume(["file1.csv", "file2.csv"], extract=dict(nrows=50))
```

If you don't want to execute the entire glider pipeline in parallel, you can
also branch into parallel execution utilizing a parallel push node as in the
following example:

```python
glider = Glider(
    RowCSVExtractor("extract", nrows=60)
    | ProcessPoolPush("push", split=True)
    | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
)
glider.consume(["infile.csv"])
```

The above example will extract 60 rows from a CSV and then push equal slices
to the logging nodes in parallel processes. Using split=False (default) would
have passed the entire 60 rows to each logging node in parallel
processes. Note that once you branch off into processes there is currently no
way to reduce/join the glider pipeline back into the original process and
resume single-process operation on the multiprocessed results. However, that
can be achieved with threads if necessary:

```python
glider = Glider(
    RowCSVExtractor("extract", nrows=60)
    | ThreadPoolPush("push", split=True)
    | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
    | ThreadReducer("reducer")
    | LoggingLoader("loadall")
)
glider.consume(["infile.csv"])
```

The above code will split the data and push to the first 3 logging nodes in
multiple threads. The ThreadReducer won't push until all of the previous nodes
have finished, and then the final logging node will print all of the results.

At this point you may be confused about the various ways you can attempt parallel processing
using Glide, so lets summarize:

- Method 1: Parallelization within nodes such as DataFrameProcessPoolTransformer 
- Method 2: Completely parallel glider pipelines via ParaGliders
- Method 3: Branched parallelism using parallel push nodes such as ProcessPoolPush or ThreadPoolPush

Each has its own utility and/or quirks. Method 1 is perhaps the most
straightforward since you return to single process operation after the node is
done doing whatever it needed to do in parallel, though the shuffling of data
to/from subprocesses is not without cost. Method 2 may be useful and easy to
understand in certain cases as well. Method 3 can lead to more
complex/confusing flows and should likely only be used towards the end of
glider pipelines to branch the output in parallel, such as if writing to
several databases in parallel.

> **Note:** combining the approaches may not work and has not been tested.

Creating Nodes
--------------

There are some small differences between Glide and Consecution. Namely,
you must inherit from the Glide Node class (vs. the Consecution Node
class), and you must define a run() method (vs. process() for Consecution)
that takes at least one positional argument for the data being pushed to it.

A simple example would be:

```python
class ExampleTransformer(Node):
    def run(self, item):
        # Do something to item here
        self.push(item)
```

Note that for ease of development Glide will automatically "listify"
node inputs. Essentially Pandas objects, list/tuple-like objects, and
generators all get passed through as is, but something like a string would get
turned into ["some string"]. The goal is to allow for safe iteration over the
input in generic nodes that may be asked to repeat their logic for each item
in the list.

Earlier we mentioned node context. This comes into play when run() is called
on the node, as the required and optional parts of the context are inferred from
the positional and keyword args of run(). Take for example:

```python
class ExampleTransformer(Node):
    def run(self, item, conn, chunksize=None, **kwargs):
        # Do something to item here
        self.push(item)
```

All nodes expect their first positional arg to be the data going through the pipeline. This
node also requires a 'conn' argument, and has an optional 'chunksize' argument. These values
can be filled in from the following inputs in the priority order:

1. Context args passed to consume for the node
2. Default context set on the node at init time
3. Global pipeline state passed via global_state. This only works for positional args currently.

Documentation
-------------

[Documentation](https://glide-etl.readthedocs.io/en/latest/) is a work in
progress. You can supplement this by perusing tests and becoming familiar with
[Consecution](https://github.com/robdmc/consecution), which is a core
dependency and has some of the same concepts/syntax used in forming pipelines
with Glide.

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

> **Note:** As of mid 2019, Consecution does not appear to be under active
development. The features Glide relies on (basically some syntactic sugar and
graph creation/traversal) seem stable but could also be rewritten if this
dependency becomes a limitation.
