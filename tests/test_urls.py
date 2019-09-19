from .test_utils import *
from glide import *


def test_url_str_extract_and_load():
    glider = Glider(URLExtractor("extract") | URLLoader("load"))
    urls = [
        "https://jsonplaceholder.typicode.com/todos/1",
        "https://jsonplaceholder.typicode.com/todos/2",
    ]
    glider.consume(
        urls,
        extract=dict(push_type="json"),
        load=dict(
            url="https://jsonplaceholder.typicode.com/todos",
            data_param="json",
            headers={"Content-type": "application/json; charset=UTF-8"},
        ),
    )


def test_url_dict_extract_and_load():
    glider = Glider(URLExtractor("extract") | URLLoader("load"))
    urls = [
        dict(method="GET", url="https://jsonplaceholder.typicode.com/todos/1"),
        dict(method="GET", url="https://jsonplaceholder.typicode.com/todos/2"),
    ]
    glider.consume(
        urls,
        extract=dict(push_type="json"),
        load=dict(
            url="https://jsonplaceholder.typicode.com/todos",
            data_param="json",
            headers={"Content-type": "application/json; charset=UTF-8"},
        ),
    )
