Glide: Easy ETL
===============

``Glide`` aims to be a simple, reusable, extensible approach to building ETL
data pipelines. It provides a suite of nodes and pipelines out of the box that
cover many common use cases, such as reading and writing data to/from SQL,
URLs, local/remote files, and email.

``Glide`` aims to have sane defaults and rely on standard, well-known Python
libraries for data processing under the hood. It strives for familiar behavior
out of the provided nodes while allowing for customization by passing
arguments through to the underlying libraries in many cases. It's also very
easy to write completely custom nodes and pipelines.
 
``Glide`` also tries to give you a lot for free, including simple parallel
processing support, a variety of ways to manage node/pipeline context, and
automatic CLI generation.

``Glide`` encourages extensions that provide nodes and pipelines for
interacting with various data sources and data processing modules. Check out
the ``glide.extensions`` module to see some currently supported extensions,
such as `Dask <https://dask.org/>`_ for scalable analytics workflows.

.. toctree::
   :maxdepth: 3

   Quickstart <quickstart.rst>
   Common Nodes <nodes>
   Common Pipelines <pipelines>
   Glide Reference <glide>
