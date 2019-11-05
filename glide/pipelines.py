"""Basic ETL pipeline templates for common nodes"""

import copy

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


class NodeTemplate:
    """A set of nodes that can be used as a template"""

    def __init__(self, nodes):
        """Create and store a deepcopy of the input nodes"""
        self.nodes = copy.deepcopy(nodes)

    def __call__(self):
        """Create and return a deepcopy of self.nodes"""
        nodes = copy.deepcopy(self.nodes)
        return nodes


class GliderTemplate:
    """A template for a Glider. It will create a new pipeline with a copy of
    its templated nodes when __call__'d.

    Parameters
    ----------
    nodes
        A top node potentially tied to other downstream nodes

    Attributes
    ----------
    nodes
        A top node potentially tied to other downstream nodes

    """

    def __init__(self, nodes):
        self.nodes = NodeTemplate(nodes)

    def __call__(self, glider=Glider):
        """Create a Glide pipeline from the nodes

        Parameters
        ----------
        glider
            The glider class to use to build the pipeline

        Returns
        -------
        A Glide pipeline

        """
        return glider(self.nodes())


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
    return GliderTemplate(extract("extract") | transform("transform") | load("load"))


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

# This will create local GliderTemplates for all combinations of
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
