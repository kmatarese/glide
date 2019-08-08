from .test_utils import *
from glide import *


def test_csv_dask_paracutor(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = DaskParaGlider(RowCSVExtractor("extract") | LoggingLoader("load"))
    glider.consume([infile], extract=dict(nrows=10))


def test_csv_process_pool_paracutor(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = ProcessPoolParaGlider(RowCSVExtractor("extract") | LoggingLoader("load"))
    glider.consume([infile], extract=dict(nrows=10))


def test_csv_thread_pool_paracutor(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = ThreadPoolParaGlider(RowCSVExtractor("extract") | LoggingLoader("load"))
    glider.consume([infile], extract=dict(nrows=10))
