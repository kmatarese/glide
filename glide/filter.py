"""A home for common filter nodes"""

from glide.core import Node
from glide.utils import find_class_in_dict, get_class_list_docstring


class DictKeyFilter(Node):
    """A node that pushes a specific value from a dict-like object"""

    def run(self, item, key):
        """Given a dict-like object, extract and push a key

        Parameters
        ----------
        item : dict-like
            The dict-like object to extract the value from
        key : hashable
            The key to extract from item

        """
        self.push(item[key])


class AttributeFilter(Node):
    """A node that pushes a specific attribute of an object"""

    def run(self, item, attribute):
        """Given an object, extract and push an attribute

        Parameters
        ----------
        item
            The object to pull the attribute from
        attribute
            The attribute to read from the object

        """
        self.push(getattr(item, attribute))


node_names = find_class_in_dict(Node, locals(), include="Filter")
if node_names:
    __doc__ = __doc__ + get_class_list_docstring("Nodes", node_names)
