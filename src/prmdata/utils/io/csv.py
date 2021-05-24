import csv
import gzip
from typing import Iterable, List


# TODO: Remove with PRMT-1798
def read_gzip_csv_file_deprecated(file_path: str) -> Iterable[dict]:
    with gzip.open(file_path, "rt") as f:
        input_csv = csv.DictReader(f)
        yield from input_csv


# TODO: Remove with PRMT-1798
def read_gzip_csv_files_deprecated(file_paths: List[str]) -> Iterable[dict]:
    for file_path in file_paths:
        yield from read_gzip_csv_file_deprecated(file_path)


def read_gzip_csv_file(file_content) -> Iterable[dict]:
    with gzip.open(file_content, mode="rt") as f:
        input_csv = csv.DictReader(f)
        yield from input_csv


def read_gzip_csv_files(file_contents) -> Iterable[dict]:
    for file_content in file_contents:
        yield from read_gzip_csv_file(file_content)
