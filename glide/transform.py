"""A home for common transform nodes"""

from tlbx import st, json, set_missing_key, update_email

from glide.core import Node
from glide.utils import find_class_in_dict, get_class_list_docstring


class Func(Node):
    """Call func with data"""

    def run(self, data, func):
        """Call func with data

        Parameters
        ----------
        data
           Data to process
        func : callable
           Function to pass data to

        """
        self.push(func(data))


class Map(Node):
    """Call the built-in map() function with func and data"""

    def run(self, data, func, as_list=False):
        """Call the built-in map() function with func and data

        Parameters
        ----------
        data
            Data to process
        func : callable
            Function to pass to map()
        as_list : bool, optional
            If True, read the map() result into a list before pushing
        """
        result = map(func, data)
        if as_list:
            result = [x for x in result]
        self.push(result)


class DictKeyTransform(Node):
    def run(self, data, drop=None, **transforms):
        """Rename/replace keys in an iterable of dicts

        Parameters
        ----------
        data
            Data to process. Expected to be a list/iterable of dict rows.
        drop : list, optional
            A list of keys to drop after transformations are complete.
        **transforms
            key->value pairs used to populate columns of each dict row. If the
            value is a callable it is expected to take the row as input and
            return the value to fill in for the key.

        """
        drop = drop or []
        assert isinstance(
            drop, (list, tuple)
        ), "drop argument must be a list/tuple of keys to drop"

        for row in data:
            assert isinstance(row, dict), "Dict rows expected, got %s" % type(row)
            for key, value in transforms.items():
                if callable(value):
                    row[key] = value(row)
                else:
                    row[key] = value

            for key in drop:
                del row[key]

        self.push(data)


class JSONDumps(Node):
    """Call json.dumps on the data"""

    def run(self, data):
        """Call json.dumps on the data and push"""
        self.push(json.dumps(data))


class JSONLoads(Node):
    """Call json.loads on the data"""

    def run(self, data):
        """Call json.loads on the data and push"""
        self.push(json.loads(data))


class EmailMessageTransform(Node):
    """Update EmailMessage objects"""

    def run(
        self,
        msg,
        frm=None,
        to=None,
        subject=None,
        body=None,
        html=None,
        attachments=None,
    ):
        """Update the EmailMessage with the given arguments

        Parameters
        ----------
        msg : EmailMessage
            EmailMessage object to update
        frm : str, optional
            Update from address
        to : str, optional
            Update to address(es)
        subject : str, optional
            Update email subject
        body : str, optional
            Update email body
        html : str, optional
            Update email html
        attachments : list, optional
            Replace the email attachments with these

        """
        update_email(
            msg,
            frm=frm,
            to=to,
            subject=subject,
            body=body,
            html=html,
            attachments=attachments,
        )
        self.push(msg)


node_names = find_class_in_dict(Node, locals(), exclude="Node")
if node_names:
    __doc__ = __doc__ + get_class_list_docstring("Nodes", node_names)
