"""A home for common transform nodes"""

from tlbx import st, json, set_missing_key, update_email

from glide.core import Node
from glide.utils import find_class_in_dict, get_class_list_docstring


class JSONDumps(Node):
    def run(self, data):
        self.push(json.dumps(data))


class JSONLoads(Node):
    def run(self, data):
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
