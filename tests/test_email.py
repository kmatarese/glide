import datetime
import logging
import smtplib
import time

from .test_utils import *
from glide import *

imapclient_logger = logging.getLogger("imapclient")
imapclient_logger.setLevel(logging.INFO)

DEFAULT_EXTRACT_ARGS = dict(
    host=test_config["IMAPHost"],
    username=test_config["IMAPUsername"],
    password=test_config["IMAPPassword"],
)

DEFAULT_LOAD_ARGS = dict(
    host=test_config["SMTPHost"],
    port=test_config["SMTPPort"],
    username=test_config["SMTPUsername"],
    password=test_config["SMTPPassword"],
)


def email_pipeline(
    extract=None,
    transform=None,
    load=None,
    extractor=EmailExtractor,
    transformer=PlaceholderNode,
    loader=EmailLoader,
):
    glider = Glider(extractor("extract") | transformer("transform") | loader("load"))

    since = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%d-%b-%Y")
    criteria = ["SINCE", since]

    glider.consume(
        [criteria], extract=extract or {}, transform=transform or {}, load=load or {}
    )


# -------- Extract tests


def test_email_extract_default():
    extract = DEFAULT_EXTRACT_ARGS.copy()
    extract.update(dict(limit=2))
    email_pipeline(extract=extract, loader=Printer)


def test_email_extract_message_ids():
    extract = DEFAULT_EXTRACT_ARGS.copy()
    extract.update(dict(push_type="message_id", limit=2))
    email_pipeline(extract=extract, loader=Printer)


def test_email_extract_bodies():
    extract = DEFAULT_EXTRACT_ARGS.copy()
    extract.update(dict(push_type="body", limit=2))
    email_pipeline(extract=extract, loader=Printer)


def test_email_extract_attachments():
    extract = DEFAULT_EXTRACT_ARGS.copy()
    extract.update(dict(push_type="attachment", limit=2))
    email_pipeline(extract=extract, loader=Printer)


def test_email_extract_all():
    extract = DEFAULT_EXTRACT_ARGS.copy()
    extract.update(dict(push_type="all", limit=2))
    email_pipeline(extract=extract, loader=Printer)


# TODO: Need to find a test client with sort capability.
# def test_email_extract_sorted():
#     extract = DEFAULT_EXTRACT_ARGS.copy()
#     extract.update(dict(sort=['REVERSE DATE'], limit=2))
#     email_pipeline(extract=extract, loader=Printer)


# -------- Load tests


def init_smtp_client(client):
    client.ehlo()
    client.starttls()
    client.ehlo()
    client.login(test_config["SMTPUsername"], test_config["SMTPPassword"])


def test_email_load_messages():
    extract = DEFAULT_EXTRACT_ARGS.copy()
    extract.update(dict(limit=1))
    transform = dict(
        frm=test_config["SMTPUsername"],
        to=test_config["EmailDestination"],
        subject="test_email_load_message: %s" % time.time(),
    )
    load = DEFAULT_LOAD_ARGS.copy()
    email_pipeline(
        extract=extract,
        transform=transform,
        load=load,
        transformer=EmailMessageTransformer,
    )


def test_email_load_messages_with_client():
    extract = DEFAULT_EXTRACT_ARGS.copy()
    extract.update(dict(limit=1))
    transform = dict(
        frm=test_config["SMTPUsername"],
        to=test_config["EmailDestination"],
        subject="test_email_load_messages_with_client: %s" % time.time(),
    )
    with smtplib.SMTP(test_config["SMTPHost"], port=test_config["SMTPPort"]) as client:
        init_smtp_client(client)
        load = dict(client=client)
        email_pipeline(
            extract=extract,
            transform=transform,
            load=load,
            transformer=EmailMessageTransformer,
        )


def test_email_load_messages_format_body():
    extract = DEFAULT_EXTRACT_ARGS.copy()
    extract.update(dict(push_type="body", limit=2))
    with smtplib.SMTP(test_config["SMTPHost"], port=test_config["SMTPPort"]) as client:
        init_smtp_client(client)

        load = dict(
            client=client,
            frm=test_config["SMTPUsername"],
            to=test_config["EmailDestination"],
            subject="test_email_load_messages_format_body: %s" % time.time(),
            attach_as="body",
            formatter=lambda x: "TEST:\n" + x,
        )
        email_pipeline(
            extract=extract,
            load=load,
            transform=dict(attribute="payload"),
            transformer=AttributeFilterNode,
        )


def test_email_load_messages_format_html():
    extract = DEFAULT_EXTRACT_ARGS.copy()
    extract.update(dict(push_type="body", limit=2))
    with smtplib.SMTP(test_config["SMTPHost"], port=test_config["SMTPPort"]) as client:
        init_smtp_client(client)

        load = dict(
            client=client,
            frm=test_config["SMTPUsername"],
            to=test_config["EmailDestination"],
            subject="test_email_load_messages_format_body: %s" % time.time(),
            attach_as="html",
            formatter=lambda x: x + "<p>Test</p>",
        )
        email_pipeline(
            extract=extract,
            load=load,
            transform=dict(attribute="payload"),
            transformer=AttributeFilterNode,
        )


def test_email_load_data_to_attachment(rootdir):
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(FileExtractor("extract") | EmailLoader("load"))

    with smtplib.SMTP(test_config["SMTPHost"], port=test_config["SMTPPort"]) as client:
        init_smtp_client(client)
        load = dict(
            client=client,
            frm=test_config["SMTPUsername"],
            to=test_config["EmailDestination"],
            subject="test_email_load_messages_as_attachment: %s" % time.time(),
            attach_as="attachment",
            attachment_name="attachment.csv",
        )
        glider.consume([infile], load=load)
