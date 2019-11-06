Creating Pipelines
==================

A ``Glider`` is a pipeline of ``Node`` objects wired together in a DAG. It
accepts input data in its ``consume`` method. For example::

    glider = Glider(
        MyExtractNode("extract")
        | MyTransformNode("transform")
        | MyLoadNode("load")
    )
    glider.consume(data)

The ``consume`` method accepts node_name -> node_context keyword arguments
that can update the context of the pipeline's nodes for the duration of the
``consume`` call. For example, if ``MyLoadNode`` in the example above had
an argument called ``foo`` in its ``run`` method, you could set the value of
``foo`` for a particular pipeline run as follows::

    glider.consume(data, load=dict(foo="bar"))

Pipelines can be templated as well for easy reuse. Any node can be replaced by name::

    glider = Glider(
        PlaceholderNode("extract")
        | CSVLoad("load")
    )
    glider["extract"] = CSVExtract("extract")
    glider.consume(...)

Or reuse an existing pipeline structure with ``GliderTemplate``::

    template = GliderTemplate(
        CSVExtract("extract")
        | CSVLoad("load")
    )
    glider = template() # Copy of pipeline created with each call
    glider.consume(...)

Complex Pipelines
-----------------

Glide's ``Node`` class has an ``add_downstream`` method that it inherits from
Consecution's Node class. You can use this to form more complex topologies, such
as in the following example::

    def parity_router(num):
        if num % 2 == 0:
            return "even"
        return "odd"

    def threshold_router(num):
        prepend = "odd"
        if num % 2 == 0:
            prepend = "even"
        if num >= 10:
            return "%s_large" % prepend
        return "%s_small" % prepend

    glider = Glider(
        CSVExtract("extract", nrows=40)
        | IterPush("iter")
        | [
            parity_router,
            (
                Print("even")
                | [threshold_router, Print("even_large"), Print("even_small")]
            ),
            (
                Print("odd")
                | [threshold_router, Print("odd_large"), Print("odd_small")]
            ),
        ]
    )

    large = Print("large")
    small = Print("small")
    reducer = Reduce("reduce")
    combined = LenPrint("combined")

    large.add_downstream(reducer)
    small.add_downstream(reducer)
    reducer.add_downstream(combined)

    glider["even_large"].add_downstream(large)
    glider["odd_large"].add_downstream(large)
    glider["even_small"].add_downstream(small)
    glider["odd_small"].add_downstream(small)

    glider.consume(range(20))
    glider.plot("pipeline.png") # View hierarchy if you have GraphViz installed

This also takes advantage of Consecution's router functionality to use
``parity_router`` and ``threshold_router`` to steer data through the pipeline.

CLI Generation
==============

With Glide you can create parameterized command line scripts from any pipeline
with a simple decorator::

    glider = Glider(
        SQLLoad("extract")
        | SQLExtract("load")
    )

    @glider.cli()
    def main(data, node_contexts):
        glider.consume(data, **node_contexts)

    if __name__ == "__main__":
        main()

The script arguments, their types, and whether they are required or not is all
inferred by inspecting the ``run`` arguments on the nodes of the pipeline and
prefixing the node name. For example, ``SQLLoad`` requires a ``conn`` and a
``table`` argument, as well as having a few optional arguments. Since the node
is named "load", the CLI will automatically generate required args called
``--load_conn`` and ``--load_table``. Additionally, the default help strings are
extracted from the run() method documentation if you use numpy docstring
format.

By default, the first positional argument(s) expected on the CLI are used to
populate the ``data`` argument. If the top node of your pipeline is a subclass
of ``NoInputNode`` then the CLI will automatically skip the ``data`` CLI arg and not
try to pass any data as the first positional argument to the wrapped function.

Let's ignore the fact that you can't pass a real
database connection object on the command line for a second and see how you
would run this script:
.. code-block:: console

    $ python my_script.py "select * from input_table limit 10" \
    --extract_conn foo \
    --load_conn bar \
    --load_table output_table 

To pass multiple inputs to ``data`` you would simply use space-separated
positional arguments:
.. code-block:: console

    $ python my_script.py "sql query 1" "sql query 2" \
    --extract_conn foo \
    --load_conn bar \
    --load_table output_table 

One way to populate the ``conn`` arguments of pipeline nodes is to define it in
the ``global_state`` or in the node initialization calls. In either case it is
no longer considered a *required* command line argument. So the following
would work::

    glider = Glider(
        SQLExtract("extract")
        | SQLLoad("load"),
        global_state=dict(conn=get_my_db_conn())
    )

.. code-block:: console

    $ python my_script.py "select * from input_table limit 10" \
    --load_table output_table 

Blacklisting Args
-----------------

In the previous example it is no longer necessary to even have the
node-specific connection arguments show up on the command line (such as in
--help output). You can blacklist the arg from ever getting put into the CLI
as follows::

    @glider.cli(blacklist=["conn"])
    def main(data, node_contexts):
        glider.consume(data, **node_contexts)

Or, if you just wanted to blacklist an argument that appears in multiple nodes
from a single node (such as the ``conn`` argument required in both the extract
and load nodes in this example), you could be more explicit and prefix the
node name::

    @glider.cli(blacklist=["load_conn"])
    def main(data, node_contexts):
        glider.consume(data, **node_contexts)

That would remove ``load_conn`` from the CLI but not ``extract_conn``.

Custom Arguments
----------------

You can also override or add any argument you want using the ``Arg`` class which
takes the standard ``argparse`` arguments::

    from glide.core import Glider, Arg
    
    glider = ...
    
    @glider.cli(Arg("--load_table", required=False, default="output_table"))
    def main(data, node_contexts):
        glider.consume(data, **node_contexts)

And now, assuming you had used the ``Glider`` with ``conn`` passed in the
``global_state``, you could simple do:
.. code-block:: console

    $ python my_script.py "select * from input_table limit 10"

You can override the ``data`` positional argument in this way too if you want to
change the type/requirements::

    @glider.cli(Arg("data", type=str, default="some default sql query"))
    def main(data, node_contexts):
        glider.consume(data, **node_contexts)

Parent CLIs
-----------

If you want to inherit or share arguments you can accomplish that using the
``Parent`` and ``Arg`` decorators together. These are using
`climax <https://github.com/miguelgrinberg/climax/>`_.  under the hood, which
is utilizing ``argparse``. For example, the following script inherits a
``--dry_run`` boolean CLI flag::

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

Argument Injection and Clean Up
-------------------------------

The script decorator also has the ability to inject values into arguments
based on the result of a function, and call clean up functions for the various
injected arguments. The following example shows two useful cases::

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

Here we use the ``inject`` decorator argument and pass a dictionary that maps
injected argument names to functions that return the values. We inject a ``data``
arg and a ``conn`` arg and neither are necessary for the command line. This
automatically blacklists those args from the command line as well. Since we
added the ``load_table`` arg and gave it a default as well, we can now simply
run:
.. code-block:: console

    $ python my_script.py

.. note:: Injected args are also passed to the wrapped function as keyword args. 

.. note:: If an injected argument name is mapped to a non-function via
   ``inject`` the value will be used as is. The main difference is those values are
   interpreted as soon as the module is loaded (when the decorator is init'd). If
   that is not desirable, pass a function as shown above which will only be
   executed once the decorated function is actually called. Injected
   RuntimeContexts and other objects that are not a ``types.FunctionType`` or
   ``functools.partial`` are passed through as-is.

The ``cleanup`` decorator argument takes a dictionary that maps argument names to
callables that accept the argument value to perform some clean up. In this
case, it closes the database connection after the wrapped method is complete.

Boolean Args
------------

Node ``run`` args whose default is a boolean value will be converted to boolean
flags on the CLI. If the default is ``True``, the flag will invert the logic of
the flag and prepend ``no_`` to the beginning of the arg name for clarity. 

For example, the ``SQLLoad`` node has a ``run`` keyword arg with a default of
``commit=True``. Assuming this node was named ``load``, this will produce a CLI
flag ``--load_no_commit`` which, when passed in a terminal, will set
``commit=False`` in the node. If the default had been ``False`` the CLI arg name
would have simply been ``--load_commit`` and it would set the value to ``True``
when passed in a terminal.

This leads to more clear CLI behavior as opposed to having a flag with a
truth-like name getting a false-like result when passed in a terminal. Of
course another option would have been to define the node keyword arg as
``no_commit=False`` instead of ``commit=True``. This would also lead to
understandable CLI behavior but, in my opinion, would lead to more confusing
variable naming in your code.


Parallel Processing
===================

There are three main ways you can attempt parallel processing using Glide:

- Method 1: Parallelization *within* nodes such as ``ProcessPoolSubmit`` or a distributed processing extension such as Dask/Celery/Redis Queue
- Method 2: Completely parallel pipelines via ``ParaGliders`` (each process executes the entire pipeline)
- Method 3: Branched parallelism using parallel push nodes such as ``ProcessPoolPush`` or ``ThreadPoolPush``

Each has its own use cases. Method 1 is perhaps the most straightforward since
you can return to single process operation after the node is done doing whatever
it needed to do in parallel. Method 2 may be useful and easy to understand in
certain cases as well. Method 3 can lead to more complex/confusing flows and
should probably only be used towards the end of pipelines to branch the output
in parallel, such as if writing to several databases in parallel as a final
step.

Please see the quickstart or tests for examples of each method.

**Note:** Combining the approaches may not work and has not been tested. Standard
limitations apply regarding what types of data can be serialized and passed to
a parallel process.


Common Pipelines
================

``Glide`` comes with some common, templated ETL pipelines that connect
combinations of common nodes. The names are generally of the format
"Source2Destination". The names of the available pipelines are listed
in the :py:mod:`glide.pipelines` module documentation.

To use these pipelines, simply call the template to get an instance of a
Glider, such as::

    glider = File2Email()
    glider.consume([file1, file2], load=dict(client=my_smtp_cient))

By default these templated pipelines have a ``PlaceholderNode`` named
"transform" that you can easily replace once the glider is created::

    glider["transform"] = MyTransformerNode("transform")
    glider.consume(...)

You can also override the ``Glider`` class used to create the pipeline::

    glider = File2Email(glider=ProcessPoolParaGlider)

All of these templated pipelines are simply a convenience and are meant to
cover very simple cases. More often than not it's likely best to create your
own explicit pipelines.



