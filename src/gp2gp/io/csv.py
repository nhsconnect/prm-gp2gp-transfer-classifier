import csv
import gzip
from typing import Iterable


def read_gzip_csv(file_path: str) -> Iterable[dict]:
    with gzip.open(file_path, "rt") as f:
        input_csv = csv.DictReader(f)
        yield from input_csv
