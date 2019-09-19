"""Common utilities"""

# import attr
# from email.message import EmailMessage
from inspect import isgenerator
import io

# import mimetypes
# import ntpath
# import smtplib

import pandas as pd
from toolbox import st, is_str, MappingMixin

# SMTP_SSL_PORT = 465


def is_pandas(o):
    return isinstance(o, (pd.DataFrame, pd.Series, pd.Panel))


def is_file_obj(o):
    return isinstance(o, (io.TextIOBase, io.BufferedIOBase, io.RawIOBase, io.IOBase))


def iterize(o):
    """Automatically wrap certain objects that you would not normally process item by item"""
    if (
        is_pandas(o)
        or is_str(o)
        or is_file_obj(o)
        or isinstance(o, dict)
        or callable(o)
    ):
        return [o]
    return o


# @attr.s(kw_only=True)
# class EmailPayload(MappingMixin):
#     payload = attr.ib()
#     content_type = attr.ib()
#     charset = attr.ib()


# def extract_email_payload(msg, decode=True):
#     """Extract and decode the payload of an email.message.EmailMessage object

#     Parameters
#     ----------
#     msg : EmailMessage
#         The message to extract the payload from
#     decode : bool, optional
#         The decode flag to pass to get_payload

#     Returns
#     -------
#     A list of dicts with content_type/charset/payload items

#     """

#     def decode_if_necessary(payload, charset):
#         # decode=True still returns encoded bytes, but just decodes the
#         # message contents from transfer encoding, such as if they are base64
#         # encoded.
#         charset = charset or 'utf8' # Fallback to utf8 if no charset specified
#         if (not decode) or (not isinstance(payload, bytes)):
#             return payload
#         return payload.decode(charset)

#     if msg.is_multipart():
#         parts = []
#         for part in msg.walk():
#             if 'multipart' in part.get_content_type().lower():
#                 continue
#             charset = part.get_content_charset()
#             payload = part.get_payload(decode=decode)
#             payload = decode_if_necessary(payload, charset)
#             parts.append(EmailPayload(content_type=part.get_content_type(),
#                                       charset=charset,
#                                       payload=payload))
#         return parts
#     else:
#         # When is_multipart is False, payload is a string
#         charset = msg.get_content_charset()
#         payload = msg.get_payload(decode=decode)
#         payload = decode_if_necessary(payload, charset)
#         return [EmailPayload(content_type=msg.get_content_type(),
#                              charset=charset,
#                              payload=payload)]


# def add_email_attachments(msg, attachments):
#     for attachment in (attachments or []):
#         assert ntpath.isfile(attachment), 'Email attachments must be valid file paths'
#         filename = ntpath.basename(attachment)

#         ctype, encoding = mimetypes.guess_type(attachment)
#         if ctype is None or encoding is not None:
#             # No guess could be made, or the file is encoded (compressed), so
#             # use a generic bag-of-bits type.
#             ctype = 'application/octet-stream'
#         maintype, subtype = ctype.split('/', 1)

#         with open(attachment, 'rb') as fp:
#             msg.add_attachment(fp.read(),
#                                maintype=maintype,
#                                subtype=subtype,
#                                filename=filename)

# def create_email(frm, to, subject, body=None, html=None, attachments=None):
#     msg = EmailMessage()
#     msg['From'] = frm
#     msg['To'] = to if isinstance(to, str) else ', '.join(to)
#     msg['Subject'] = subject
#     msg.preamble = 'You will not see this in a MIME-aware mail reader.\n'
#     msg.set_content(body or '')

#     if html is not None:
#         msg.add_alternative(html, subtype='html')

#     add_email_attachments(msg, attachments)

#     return msg

# def send_email(msg, client=None, host=None, port=None, username=None, password=None, smtp_class=smtplib.SMTP, debug=False):
#     if client:
#         client.send_message(msg)
#         return

#     assert (host and port and username and password), \
#         "Must pass host/port/username/password for SMTP connection"

#     if port == SMTP_SSL_PORT:
#         smtp_class = smtplib.SMTP_SSL
#     with smtp_class(host, port=port) as client:
#         client.ehlo()
#         if smtp_class == smtplib.SMTP:
#             client.starttls()
#             client.ehlo()
#         client.login(username, password)
#         client.set_debuglevel(debug)
#         client.send_message(msg)
