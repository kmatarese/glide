"""Basic ETL pipeline templates for common nodes"""

from functools import partial

from tlbx import st

from glide.core import Glider, PlaceholderNode
from glide.extract import (
    SQLExtract,
    SQLParamExtract,
    CSVExtract,
    EmailExtract,
    FileExtract,
    URLExtract,
)
from glide.load import SQLLoad, CSVLoad, EmailLoad, FileLoad, URLLoad
from glide.utils import iterize, get_class_list_docstring


class GliderTemplate:
    """A 'partial' for a Glider. It must be passed functools.partial nodes
    that it will create when __call__'d.

    Note
    ----
    This currently does not support lists of lists for node levels.

    Parameters
    ----------
    node_levels*
        An iterable of functools.partial nodes

    Attributes
    ----------
    node_levels
        An iterable of functools.partial nodes

    """

    def __init__(self, *node_levels):
        self.node_levels = node_levels

    def __call__(self, glider=Glider):
        """Create a Glide pipeline from the partial nodes

        Parameters
        ----------
        glider
            The glider class to use to build the pipeline

        Returns
        -------
        A Glide pipeline

        """
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


def basic_glider(
    extract=PlaceholderNode, transform=PlaceholderNode, load=PlaceholderNode
):
    """Convenience function to produce a basic ETL template

    Parameters
    ----------
    extract : type, optional
        A Node class to use as the extractor
    transform : type, optional
        A Node class to use as the transformer
    load : type, optional
        A Node class to use as the loader

    Returns
    -------
    A GliderTemplate that can be called to produce Gliders from the template.

    """
    return GliderTemplate(
        partial(extract, "extract"),
        partial(transform, "transform"),
        partial(load, "load"),
    )


GLIDER_TEMPLATE_DATA = {
    "": {
        "extract": [
            ("SQL", SQLExtract),
            ("SQLParam", SQLParamExtract),
            ("CSV", CSVExtract),
        ],
        "load": [("SQL", SQLLoad), ("CSV", CSVLoad)],
    }
}

# This will create local GliderTemplate 'partials' for all combinations of
# extractors and loaders in the above data structure
for data_format, template in GLIDER_TEMPLATE_DATA.items():
    extractors = template["extract"]
    loaders = template["load"]
    for etype, extractor in extractors:
        for ltype, loader in loaders:
            name = "%s%s2%s" % (data_format, etype, ltype)
            locals()[name] = basic_glider(extract=extractor, load=loader)

File2File = basic_glider(extract=FileExtract, load=FileLoad)
File2Email = basic_glider(extract=FileExtract, load=EmailLoad)
File2URL = basic_glider(extract=FileExtract, load=URLLoad)

Email2Email = basic_glider(extract=EmailExtract, load=EmailLoad)
Email2File = basic_glider(extract=EmailExtract, load=FileLoad)

URL2Email = basic_glider(extract=URLExtract, load=EmailLoad)
URL2File = basic_glider(extract=URLExtract, load=FileLoad)
URL2URL = basic_glider(extract=URLExtract, load=URLLoad)

template_names = []
for key, value in locals().copy().items():
    if isinstance(value, GliderTemplate):
        template_names.append(key)

if template_names:
    __doc__ = __doc__ + get_class_list_docstring("Pipelines", template_names)
