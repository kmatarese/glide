Common Nodes
============

``Glide`` comes with a suite of nodes to handle common data processing
tasks. The easiest way to view the options and understand their behavior is to
peruse the module documentation and/or review the source code for each node.

- For extractor nodes, such as SQL/CSV/Excel/File/URL extractors, see :py:mod:`glide.extract`.
- For transformer nodes, see :py:mod:`glide.transform`.
- For filter nodes, see :py:mod:`glide.filter`.
- For loader nodes, such as SQL/CSV/Excel/File/URL loaders, see :py:mod:`glide.load`.
- For some additional flow control nodes see :py:mod:`glide.core`.

Keep in mind it's very easy to write your own nodes. If you don't see
something you want, or you want slightly different behavior, create your own
node. If you think it's something that could benefit other users please
contribute!
