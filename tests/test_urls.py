from fake_useragent import UserAgent

from .test_utils import *
from glide import *


def test_url_str_extract_text():
    glider = Glider(URLExtract("extract") | JSONLoads("json") | PrettyPrint("load"))
    reqs = ["https://jsonplaceholder.typicode.com/todos/1"]
    glider.consume(
        reqs, extract=dict(data_type="text", headers={"User-Agent": UserAgent().random})
    )


def test_url_str_extract_and_load():
    glider = Glider(URLExtract("extract") | URLLoad("load"))
    reqs = [
        "https://jsonplaceholder.typicode.com/todos/1",
        "https://jsonplaceholder.typicode.com/todos/2",
    ]
    glider.consume(
        reqs,
        extract=dict(data_type="json", headers={"User-Agent": UserAgent().random}),
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
        extract=dict(data_type="json", headers={"User-Agent": UserAgent().random}),
        load=dict(
            url="https://jsonplaceholder.typicode.com/todos",
            data_param="json",
            headers={"Content-type": "application/json; charset=UTF-8"},
        ),
    )


def test_url_paging():
    glider = Glider(URLExtract("extract") | LenPrint("length"))
    reqs = ["https://reqres.in/api/users"]

    def handle_paging(result, request):
        page = result["data"]
        last = False
        if result["page"] == result["total_pages"]:
            last = True
        else:
            page_params = dict(page=result["page"] + 1)
            new_params = request.get("params", {})
            new_params.update(page_params)
            request["params"] = new_params
        return page, last

    glider.consume(
        reqs,
        extract=dict(
            data_type="json",
            handle_paging=handle_paging,
            push_pages=True,
            headers={"User-Agent": UserAgent().random},
            params=dict(page=0),
        ),
    )
