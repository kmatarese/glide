from .test_utils import *

from consecutils import *


def test_csv_dask_paracutor(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    pipeline = DaskParacutor(RowCSVExtractor("extract") | LoggingLoader("load"))
    pipeline.consume([infile], extract=dict(nrows=10))


def test_csv_process_pool_paracutor(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    pipeline = ProcessPoolParacutor(RowCSVExtractor("extract") | LoggingLoader("load"))
    pipeline.consume([infile], extract=dict(nrows=10))


def test_csv_thread_pool_paracutor(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    pipeline = ThreadPoolParacutor(RowCSVExtractor("extract") | LoggingLoader("load"))
    pipeline.consume([infile], extract=dict(nrows=10))
