from .test_utils import *
from glide import *


def test_file_str_extract(rootdir):
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(FileExtractor("extract") | Printer("load"))
    glider.consume([infile], extract=dict(limit=5, push_lines=True))


def test_file_buffer_extract(rootdir):
    infile, _ = get_filenames(rootdir, "csv")
    with open(infile, "r") as f:
        glider = Glider(FileExtractor("extract") | Printer("load"))
        glider.consume([f], extract=dict(limit=10, chunksize=4))


def test_file_extract_and_load(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = Glider(FileExtractor("extract") | FileLoader("load"))
    glider.consume([infile], load=dict(f=outfile))
