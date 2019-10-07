from glide.extensions.swifter import *
from ..test_utils import *


def test_swifter_csv_chunked_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | SwifterApplyTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume(
        [infile],
        extract=dict(chunksize=500),
        transform=dict(func=lower),
        load=dict(f=outfile),
    )


def test_swifter_chunked_threads_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtractor("extract")
        | SwifterApplyTransformer("transform")
        | DataFrameCSVLoader("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume(
        [infile],
        extract=dict(chunksize=500),
        transform=dict(func=lower, processes=False),
        load=dict(f=outfile),
    )
