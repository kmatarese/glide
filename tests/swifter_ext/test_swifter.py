from glide.extensions.pandas import *
from glide.extensions.swifter import *
from ..test_utils import *


def test_swifter_csv_chunked_lowercase(rootdir):
    nodes = (
        DataFrameCSVExtract("extract")
        | SwifterApply("transform")
        | DataFrameCSVLoad("load", index=False, mode="a")
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
        DataFrameCSVExtract("extract")
        | SwifterApply("transform")
        | DataFrameCSVLoad("load", index=False, mode="a")
    )
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    glider.consume(
        [infile],
        extract=dict(chunksize=500),
        transform=dict(func=lower, threads=True),
        load=dict(f=outfile),
    )
