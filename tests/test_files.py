from .test_utils import *
from glide import *


def test_csv_url_row_extract_and_load(rootdir):
    nodes = CSVExtract("extract") | CSVLoad("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    infile = "file://" + infile
    with open(outfile, "w") as f:
        glider.consume([infile], load=dict(f=f))


def test_csv_row_extract_and_load(rootdir):
    nodes = CSVExtract("extract") | CSVLoad("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], load=dict(f=f))


def test_csv_chunked_row_extract_and_load(rootdir):
    nodes = CSVExtract("extract") | CSVLoad("load")
    glider, infile, outfile = file_glider(rootdir, "csv", nodes)
    with open(outfile, "w") as f:
        glider.consume([infile], extract=dict(chunksize=100), load=dict(f=f))


def test_excel_row_extract_and_load(rootdir):
    nodes = ExcelExtract("extract") | ExcelLoad("load")
    glider, infile, outfile = file_glider(rootdir, "xlsx", nodes)
    glider.consume(
        [infile],
        extract=dict(row_limit=5, dict_rows=True),
        load=dict(f=outfile, dict_rows=True),
    )


def test_excel_row_file_extract_and_load(rootdir):
    nodes = ExcelExtract("extract") | ExcelLoad("load")
    glider, infile, outfile = file_glider(rootdir, "xlsx", nodes)
    with open(infile, "rb") as _in, open(outfile, "wb") as _out:
        glider.consume([_in], extract=dict(row_limit=10), load=dict(f=_out))


def test_excel_row_sheet_extract(rootdir):
    nodes = ExcelExtract(
        "extract", dict_rows=True, sheet_index=0, row_limit=5
    ) | PrettyPrint("load")
    glider, infile, outfile = file_glider(rootdir, "xlsx", nodes)
    glider.consume([infile])


def test_excel_row_xls_extract(rootdir):
    nodes = ExcelExtract("extract", dict_rows=True, row_limit=5) | LenPrint("load")
    glider, infile, outfile = file_glider(rootdir, "xls", nodes)
    glider.consume([infile])


def test_file_str_extract(rootdir):
    infile, _ = get_filenames(rootdir, "csv")
    glider = Glider(FileExtract("extract") | Print("load"))
    glider.consume([infile], extract=dict(limit=5, push_lines=True))


def test_file_buffer_extract(rootdir):
    infile, _ = get_filenames(rootdir, "csv")
    with open(infile, "r") as f:
        glider = Glider(FileExtract("extract") | Print("load"))
        glider.consume([f], extract=dict(limit=10, chunksize=4))


def test_file_extract_and_load(rootdir):
    infile, outfile = get_filenames(rootdir, "csv")
    glider = Glider(FileExtract("extract") | FileLoad("load"))
    glider.consume([infile], load=dict(f=outfile))
