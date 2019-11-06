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

### Questions

The GitHub issue tracker is for *bug reports* and *feature requests*. Please do
not use it to ask questions about how to use Glide.

## Good Bug Reports

Please be aware of the following things when filing bug reports:

1. Avoid raising duplicate issues. *Please* use the GitHub issue search feature
   to check whether your bug report or feature request has been mentioned in
   the past. Duplicate bug reports and feature requests are a huge maintenance
   burden on the limited resources of the project. If it is clear from your
   report that you would have struggled to find the original, that's ok, but
   if searching for a selection of words in your issue title would have found
   the duplicate then the issue will likely be closed.
2. When filing bug reports about exceptions or tracebacks, please include the
   *complete* traceback. Partial tracebacks, or just the exception text, are
   not helpful. Issues that do not contain complete tracebacks may be closed
   without warning.
3. Make sure you provide a suitable amount of information to work with.
