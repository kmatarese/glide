from .test_utils import *
from glide import *
from glide.extensions import *


def test_csv_dask_paraglider(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = DaskParaGlider(RowCSVExtractor("extract") | Logger("load"))
    glider.consume([infile], extract=dict(nrows=10))


def test_csv_process_pool_paraglider(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = ProcessPoolParaGlider(RowCSVExtractor("extract") | Logger("load"))
    glider.consume([infile], extract=dict(nrows=10))


def test_csv_thread_pool_paraglider(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = ThreadPoolParaGlider(RowCSVExtractor("extract") | Logger("load"))
    glider.consume([infile], extract=dict(nrows=10))
