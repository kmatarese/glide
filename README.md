Consecutils: Utilities for data pipelines
=========================================

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Consecutils uses a syntax similar to [Consecution](https://github.com/robdmc/consecution), which was inspired by Apache Storm Topologies.
Consecutils basically wraps the functionality of Consecution to provide some utilities out of the box:
        
- Reading/Writing to/from CSVs, Excel, or SQL Databases
- Nodes for processing row-based iterators (such as a list of dicts) or Pandas DataFrames
- Nodes utilizing [Swifter](https://github.com/jmcarpenter2/swifter) or [Dask](https://github.com/dask/dask) for DataFrame transformation
- Node and graph parallelization support via concurrent.futures Executors or Dask Client/Delayed/DataFrames
- Ability to set default node contexts which can be easily overwritten when calling pipeline.consume()

Quickstart
----------

``` {.sourceCode .python}
from consecutils import *
```

A simple example that reads a CSV and logs rows to stdout:

``` {.sourceCode .python}
pipeline = Consecutor(RowCSVExtractor("extract") | LoggingLoader("load"))
pipeline.consume(["filename.csv"])
```

An example of using and replacing placeholders in pipelines:

``` {.sourceCode .python}
pipeline = Consecutor(
    PlaceholderNode("extract")
    | DataFrameCSVLoader("load", index=False, mode="a")
)
pipeline["extract"] = DataFrameCSVExtractor("extract")
pipeline.consume(["infile.csv"], extract=dict(chunksize=100), load=dict(outfile="outfile.csv"))
```

The above example also demonstrates two separate ways to pass context to nodes:
        
1. Passing kwargs when instantiating the node. This becomes a default context
for the node any time it is used/reused.
2. Passing kwargs to consume that are node_name->node_context pairs. This context
lasts only for the the consume() call. 

Many of the provided nodes pass their context to well-documented functions, such as
DataFrame.to_csv in the case of DataFrameCSVLoader. Review the documentation/code
for each node for more detail on how args are processed and which are required. 

Back to examples...

An example applying a transformation to a DataFrame.

``` {.sourceCode .python}
def lower(s):
    return s.lower() if type(s) == str else s

pipeline = (
    DataFrameCSVExtractor("extract")
    | DataFrameApplyMapTransformer("transform")
    | DataFrameCSVLoader("load", index=False, mode="a")
)

pipeline.consume(
    ["infile.csv"],
    extract=dict(chunksize=100),
    transform=dict(func=lower),
    load=dict(outfile="outfile.csv"),
)
```

Or have the pipeline split the DataFrame and do it in parallel:

``` {.sourceCode .python}
def df_lower(df):
    df = df.applymap(lower)
    return df

pipeline = (
    DataFrameCSVExtractor("extract")
    | DataFrameProcessPoolTransformer("transform")
    | DataFrameCSVLoader("load", index=False, mode="a")
)

pipeline.consume(
    ["infile.csv"],
    transform=dict(func=df_lower),
    load=dict(outfile="outfile.csv"),
)
```

Note: there are transformer nodes for using Swifter and Dask as well if you
install those extensions.

A pipeline can also have shared/global context that can be used to populate node arguments:

``` {.sourceCode .python}
conn = get_my_sqlalchemy_conn()
sql = "select * from in_table limit 10"
pipeline = Consecutor(
    DataFrameSQLExtractor("extract")
    | DataFrameSQLLoader("load", if_exists="replace", index=False),
    global_state=dict(conn=conn) # conn will automagically be passed to any nodes that require it
)
pipeline.consume([sql], load=dict(table="out_table"))
```

Consecutils also has support for completely parallelizing pipelines using a
Paracutor instead of a Consecutor. The following code will create a process
pool and split processing of the inputs over the pool, with each process
running the entire pipeline on part of the consumed data:

``` {.sourceCode .python}
pipeline = ProcessPoolParacutor(
    RowCSVExtractor('extract') |
    LoggingLoader('load')
)
pipeline.consume(["file1.csv", "file2.csv"], extract=dict(nrows=50))
```

If you don't want to execute the entire pipeline in parallel, you can also branch into
parallel execution utilizing a parallel push node as in the following example:

``` {.sourceCode .python}
pipeline = Consecutor(
    RowCSVExtractor("extract", nrows=60)
    | ProcessPoolPush("push", split=True)
    | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
)
pipeline.consume(["infile.csv"])
```

The above example will extract 60 rows from a CSV and then push equal slices
to the logging nodes in parallel processes. Using split=False (default) would
have passed the entire 60 rows to each logging node in parallel
processes. Note that once you branch off into processes there is currently no
way to reduce/join the pipeline back into the original process and resume
single-process operation on the multiprocessed results. However, that can be
achieved with threads:

``` {.sourceCode .python}
pipeline = Consecutor(
    RowCSVExtractor("extract", nrows=60)
    | ThreadPoolPush("push", split=True)
    | [LoggingLoader("load1"), LoggingLoader("load2"), LoggingLoader("load3")]
    | ThreadReducer("reducer")
    | LoggingLoader("loadall")
)
pipeline.consume(["infile.csv"])
```

The above code will split the data and push to the first 3 logging nodes in
multiple threads. The ThreadReducer won't push until all of the previous nodes
have finished, and then the final logging node will print all of the results.

At this point you may be confused about the various ways you can attempt parallel processing
using Consecutils, so I'll summarize:

- Method 1: Completely parallel pipelines via Paracutors
- Method 2: Branched parallelism using parallel push nodes such as ProcessPoolPush or ThreadPoolPush
- Method 3: Parallelization within nodes such as DataFrameProcessPoolTransformer 

Each has its own utility and/or quirks. Method 3 is the most straightforward
IMO since you return to single process operation after the node is done doing
whatever it needed to do in parallel. Method 1 may be useful and easy to
understand in certain cases as well. Method 2 is the most confusing and should
likely only be used towards the end of pipelines to branch the output in
parallel. Note that combining the approaches may not work and has not been
tested.

Creating Nodes
--------------

There are some small differences between Consecutils and Consecution. Namely,
you must inherit from the Consecutils Node class (vs. the Consecution Node
class), and you must define a run() method (vs. process() for Consecution)
that takes at least one positional argument for the data being pushed to it.

A simple example would be:

``` {.sourceCode .python}
class ExampleTransformer(Node):
    def run(self, item):
        # Do something to item here
        self.push(item)
```

Note that for ease of development Consecutils will automatically "listify"
node inputs. Essentially Pandas objects, list/tuple-like objects, and
generators all get passed through as is, but something like a string would get
turned into ["some string"]. The goal is to allow for safe iteration over the
input if desired.

Installation
------------

Currently you must clone the git repo and then install into your python
environment as follows:

``` {.sourceCode .bash}
$ git clone https://github.com/kmatarese/consecutils
$ cd consecutils
$ make ENV=/path/to/venv install # Or "make ENV=/path/to/venv develop" for development
```

Documentation
-------------

Fantastic documentation is...not yet available. But you can get pretty far by
perusing tests and first reviewing
[Consecution](https://github.com/robdmc/consecution), which is a core
dependency and has some of the same syntax used in forming pipelines with
Consecutils.

TODOs
-------------
- More documentation
- More tests
- Pip/Conda support
- Email extractors/loaders
- URL extractors/loaders
- CI

How to Contribute
-----------------

1.  Check for open issues or open a new issue to start a discussion
    around a feature idea or a bug. 
2.  Fork [the repository](https://github.com/kmatarese/consecutils) on
    GitHub to start making your changes to the **master** branch (or
    branch off of it).
3.  Write a test which shows that the bug was fixed or that the feature
    works as expected.
4.  Send a [pull request](https://help.github.com/en/articles/creating-a-pull-request-from-a-fork). Add yourself to
    [AUTHORS](https://github.com/kmatarese/consecutils/blob/master/AUTHORS.rst).

**Note:** As of mid 2019, Consecution does not appear to be under active
development. The features Consecutils relies on (basically some syntactic
sugar and graph creation/traversal) seem stable but could also be rewritten
if this dependency becomes a limitation.
