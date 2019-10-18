from .test_utils import *
from glide import *


def test_url_str_extract_text():
    glider = Glider(URLExtract("extract") | JSONLoads("json") | PrettyPrint("load"))
    reqs = ["https://jsonplaceholder.typicode.com/todos/1"]
    glider.consume(reqs, extract=dict(data_type="text"))


def test_url_str_extract_and_load():
    glider = Glider(URLExtract("extract") | URLLoad("load"))
    reqs = [
        "https://jsonplaceholder.typicode.com/todos/1",
        "https://jsonplaceholder.typicode.com/todos/2",
    ]
    glider.consume(
        reqs,
        extract=dict(data_type="json"),
        load=dict(
            url="https://jsonplaceholder.typicode.com/todos",
            data_param="json",
            headers={"Content-type": "application/json; charset=UTF-8"},
        ),
    )


def test_url_dict_extract_and_load():
    glider = Glider(URLExtract("extract") | URLLoad("load"))
    reqs = [
        dict(method="GET", url="https://jsonplaceholder.typicode.com/todos/1"),
        dict(method="GET", url="https://jsonplaceholder.typicode.com/todos/2"),
    ]
    glider.consume(
        reqs,
        extract=dict(data_type="json"),
        load=dict(
            url="https://jsonplaceholder.typicode.com/todos",
            data_param="json",
            headers={"Content-type": "application/json; charset=UTF-8"},
        ),
    )


def test_url_paging():
    glider = Glider(URLExtract("extract") | LenPrint("length"))
    reqs = ["https://jsonplaceholder.typicode.com/posts"]
    glider.consume(
        reqs,
        extract=dict(
            data_type="json",
            page_size=2,
            # https://github.com/typicode/jsonplaceholder/issues/27
            page_size_param="_limit",
            page_offset_param="_start",
            page_request_param="params",
            page_key=lambda x: x,
            page_len=len,
            page_limit=3,
            push_pages=True,
        ),
    )
