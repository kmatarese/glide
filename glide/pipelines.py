from functools import partial

from toolbox import st

from glide.core import Glider, PlaceholderNode
from glide.extract import (
    DataFrameSQLExtractor,
    DataFrameCSVExtractor,
    DataFrameExcelExtractor,
    RowSQLExtractor,
    RowCSVExtractor,
)
from glide.load import (
    DataFrameSQLLoader,
    DataFrameCSVLoader,
    DataFrameExcelLoader,
    RowSQLLoader,
    RowCSVLoader,
)
from glide.utils import iterize


class GliderTemplate:
    """A 'partial' for a Glider. It must be passed functools.partial nodes
    that it will will create when __call__'d.

    NOTE: this currently does not support lists of lists for node levels.
    """

    def __init__(self, *node_levels):
        self.node_levels = node_levels

    def __call__(self, glider=Glider):
        nodes = self.create_nodes()
        return glider(nodes)

    def create_nodes(self):
        """Create node objects from node partials"""
        results = None
        for node_level in self.node_levels:
            node_level = iterize(node_level)
            level_nodes = []
            for node_partial in node_level:
                assert not isinstance(
                    node_partial, (list, tuple)
                ), "List of lists not supported"
                level_nodes.append(node_partial())

            if len(level_nodes) == 1:
                level_nodes = level_nodes[0]

            if not results:
                results = level_nodes
                continue

            results |= level_nodes
        return results


GLIDER_TEMPLATE_DATA = {
    "Row": {
        "extract": [("SQL", RowSQLExtractor), ("CSV", RowCSVExtractor)],
        "load": [("SQL", RowSQLLoader), ("CSV", RowCSVLoader)],
    },
    "DataFrame": {
        "extract": [
            ("SQL", DataFrameSQLExtractor),
            ("CSV", DataFrameCSVExtractor),
            ("Excel", DataFrameExcelExtractor),
        ],
        "load": [
            ("SQL", DataFrameSQLLoader),
            ("CSV", DataFrameCSVLoader),
            ("Excel", DataFrameExcelLoader),
        ],
    },
}

# This will create local GliderTemplate 'partials' for all combinations of
# extractors and loaders in the above data structure
for data_format, template in GLIDER_TEMPLATE_DATA.items():
    extractors = template["extract"]
    loaders = template["load"]
    for etype, extractor in extractors:
        for ltype, loader in loaders:
            name = "%s%s2%s" % (data_format, etype, ltype)
            locals()[name] = GliderTemplate(
                partial(extractor, "extract"),
                partial(PlaceholderNode, "transform"),
                partial(loader, "load"),
            )
