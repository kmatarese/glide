"""A home for common filter nodes"""

from glide.core import Node
from glide.utils import find_class_in_dict, get_class_list_docstring


class Filter(Node):
    """A node that only pushes if some condition is met"""

    def run(self, data, func, **kwargs):
        if func(self, data):
            self.push(data)


class DictKeyFilter(Node):
    """A node that pushes a specific value from a dict-like object"""

    def run(self, data, key):
        """Given a dict-like object, extract and push a key

        Parameters
        ----------
        data : dict-like
            The dict-like object to extract the value from
        key : hashable
            The key to extract from data

        """
        self.push(data[key])


class AttributeFilter(Node):
    """A node that pushes a specific attribute of an object"""

    def run(self, data, attribute):
        """Given an object, extract and push an attribute

        Parameters
        ----------
        data
            The object to pull the attribute from
        attribute
            The attribute to read from the object

        """
        self.push(getattr(data, attribute))


node_names = find_class_in_dict(Node, locals(), include="Filter")
if node_names:
    __doc__ = __doc__ + get_class_list_docstring("Nodes", node_names)
