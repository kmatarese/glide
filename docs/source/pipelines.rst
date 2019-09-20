Common Pipelines
================

``Glide`` comes with some common, templated ETL pipelines that connect
combinations of common nodes. The names are generally of the format
"Source2Destination", occasionally with more descriptive names for different
data types such as DataFrames. The names of the available pipelines are listed
in the :py:mod:`glide.pipelines` module documentation.

To use these pipelines, simply call the template to get an instance of a
Glider, such as::

    glider = File2Email()
    glider.consume([file1, file2], load=dict(client=my_smtp_cient))

By default these templated pipelines have a ``Placeholder`` node named
"transform" that you can easily replace once the glider is created::

    glider["transform"] = MyTransformerNode("transform")
    glider.consume(...)

You can also override the ``Glider`` class used to create the pipeline::

    glider = File2Email(glider=ProcessPoolParaGlider)

All of these templated pipelines are simply a convenience and are meant to
cover very simple cases. More often than not it's likely best to create your
own explicit pipelines.



