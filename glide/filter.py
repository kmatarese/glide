"""A home for common filter nodes"""

from glide.core import Node


class DictKeyPushNode(Node):
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


class AttributePushNode(Node):
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
