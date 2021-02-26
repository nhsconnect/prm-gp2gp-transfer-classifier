import csv
import gzip
from typing import Iterable, List


def read_gzip_csv_file(file_path: str) -> Iterable[dict]:
    with gzip.open(file_path, "rt") as f:
        input_csv = csv.DictReader(f)
        yield from input_csv


def read_gzip_csv_files(file_paths: List[str]) -> Iterable[dict]:
    for file_path in file_paths:
        yield from read_gzip_csv_file(file_path)
