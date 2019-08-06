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

``` {.sourceCode .python}
>>> from consecutils import *
>>> pipeline = Consecutor(RowCSVExtractor("extract") | LoggingLoader("load"))
>>> pipeline.consume(["filename.csv"])
```

TODO: Add more examples
- dataframe pipelines
- how to pass context values to nodes
- global context
- Consecutor vs Paracutor
- parallel transformer nodes

Note: As of mid 2019, Consecution does not appear to be under active
development. The features Consecutils relies on (basically some syntactic
sugar and graph creation/traversal) would likely be trivial to replace/rewrite
if this dependency becomes a limitation.


Installation
------------

Currently you must clone the git repo and then install into your python environment as follows:

``` {.sourceCode .bash}
$ git clone https://github.com/kmatarese/consecutils
$ cd consecutils
$ make ENV=/path/to/venv install # Or "make ENV=/path/to/venv develop" for development
```

TODOs
-------------
- More documentation
- More tests
- Pip support
- Email extractors/loaders
- URL extractors/loaders
- CI


Documentation
-------------

Fantastic documentation is...not yet available. But you can get pretty far by perusing tests and first reviewing
[Consecution](https://github.com/robdmc/consecution), which is a core dependency and has some of the same syntax used
in forming pipelines with Consecutils. 

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
